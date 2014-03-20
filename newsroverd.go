package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/animezb/newsrover"
	"github.com/animezb/newsroverd/sinks"
	_ "github.com/animezb/newsroverd/sinks/elasticsink"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "config", "./roverdconf.json", "Configuration file path.")
}

type SinkConf struct {
	Name    string          `json:"name"`
	Options json.RawMessage `json:"options"`
}

type RoverDConf struct {
	LogFile string                  `json:"log"`
	Rovers  []newsrover.RoverConfig `json:"newsgroups"`
	Sinks   []SinkConf              `json:"sinks"`
}

func ctrlc(stop chan<- bool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		forceExit := false
		for sig := range c {
			if sig == os.Interrupt || sig == syscall.SIGTERM {
				if forceExit {
					os.Exit(2)
				} else {
					go func() {
						stop <- true
					}()
					forceExit = true
				}
			}
		}
	}()
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var conf RoverDConf
	var logStream io.Writer

	if config, err := ioutil.ReadFile(configFile); err != nil {
		fmt.Printf("Error: Failed to open configuration file %s. (%s)\n", configFile, err.Error())
		os.Exit(1)
	} else {
		if err := json.Unmarshal(config, &conf); err != nil {
			fmt.Printf("Error: Failed to parse configuration file %s. (%s)\n", configFile, err.Error())
			os.Exit(1)
		}
	}

	if conf.LogFile != "" {
		logfile, err := os.OpenFile(conf.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err == nil {
			defer logfile.Close()
			logStream = logfile
		} else {
			fmt.Printf("Failed to open log file. Writing log to stdout.\n")
		}
	}
	if logStream == nil {
		logStream = os.Stdout
	}

	generalLog := log.New(logStream, "[NewsRoverD]", log.LstdFlags)
	if len(conf.Rovers) == 0 {
		generalLog.Println("No newgroups configured, no work to do. Quitting...")
		return
	}

	sinks.Register("standard", func(config json.RawMessage) (newsrover.Sink, error) {
		return &newsrover.StdSink{}, nil
	})

	newsSinks := make([]newsrover.Sink, 0, 4)
	for _, c := range conf.Sinks {
		if s, err := sinks.CreateSink(c.Name, c.Options); err == nil {
			s.SetLogger(log.New(logStream, "", log.LstdFlags))
			newsSinks = append(newsSinks, s)
		} else {
			generalLog.Printf("Error with sink %s: %s", c.Name, err.Error())
		}
	}

	generalLog.Printf("-------------")
	generalLog.Printf("Starting NewsRoverd")
	generalLog.Printf("-------------")

	if len(newsSinks) == 0 {
		generalLog.Println("No sinks configured, no where to send work to. Quitting...")
		return
	}

	generalLog.Printf("Loaded %d sinks.", len(newsSinks))

	rovers := make([]*newsrover.Rover, 0, 4)
	for _, c := range conf.Rovers {
		if c.SSL {
			if r, err := newsrover.NewRoverSsl(c.Host, c); err == nil {
				rovers = append(rovers, r)
				r.SetLogger(log.New(logStream, "", log.LstdFlags))
			} else {
				generalLog.Printf("Failed to start rover on %s. (%s)", c.Group, err.Error())
				os.Exit(1)
			}
		} else {
			if r, err := newsrover.NewRover(c.Host, c); err == nil {
				rovers = append(rovers, r)
				r.SetLogger(log.New(logStream, "", log.LstdFlags))
			} else {
				generalLog.Printf("Failed to start rover on %s. (%s)", c.Group, err.Error())
				os.Exit(1)
			}
		}
	}

	generalLog.Printf("Loaded %d groups.", len(rovers))

	for _, rov := range rovers {
		for _, sink := range newsSinks {
			rov.AddSink(sink)
		}
	}

	var runningWg sync.WaitGroup
	for _, sink := range newsSinks {
		runningWg.Add(1)
		go func(sink newsrover.Sink) {
			defer runningWg.Done()
			sink.Serve()
			for _, rov := range rovers {
				rov.RemoveSink(sink)
			}
			generalLog.Printf("Closed sink %s.", sink.Name())
		}(sink)
	}

	for _, rov := range rovers {
		runningWg.Add(1)
		go func(rov *newsrover.Rover) {
			defer runningWg.Done()
			for i := 0; i < 4; i++ {
				if i > 0 {
					generalLog.Printf("Restarting rover on newsgroup %s.", rov.Group())
				}
				if err := rov.Serve(); err != nil {
					generalLog.Printf("Rover on newsgroup %s crashed. %s", rov.Group(), err.Error())
					time.Sleep(10 * time.Second)
				} else {
					break
				}
			}
			generalLog.Printf("Closed rover on newsgroup %s.", rov.Group())
		}(rov)
	}
	quitChan := make(chan bool)
	ctrlc(quitChan)
	runningWg.Add(1)
	go func() {
		<-quitChan
		generalLog.Printf("Stopping newsroverd (rovers)...")
		for _, rov := range rovers {
			rov.Stop()
		}
		time.Sleep(1 * time.Microsecond) // Yield
		generalLog.Printf("Stopping newsroverd (sinks)...")
		for _, sink := range newsSinks {
			sink.Stop()
		}
		runningWg.Done()
	}()

	runningWg.Wait()
	generalLog.Printf("Bye.")
}
