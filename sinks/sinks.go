package sinks

import (
	"encoding/json"
	"fmt"
	"github.com/animezb/newsrover"
)

var sinks map[string]func(json.RawMessage) (newsrover.Sink, error) = make(map[string]func(json.RawMessage) (newsrover.Sink, error))

func Register(name string, entry func(json.RawMessage) (newsrover.Sink, error)) {
	sinks[name] = entry
}

func CreateSink(name string, config json.RawMessage) (newsrover.Sink, error) {
	if f, ok := sinks[name]; ok {
		return f(config)
	} else {
		return nil, fmt.Errorf("Failed to initiate sink %s. Sink not registered.", name)
	}
}
