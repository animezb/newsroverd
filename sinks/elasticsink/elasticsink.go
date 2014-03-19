package elasticsink

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/animezb/goes"
	"github.com/animezb/newsrover"
	"github.com/animezb/newsroverd/extract"
	"github.com/animezb/newsroverd/sinks"
	"github.com/golang/groupcache/lru"
	"github.com/sureshsundriyal/murmur3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	lruSize     = 2048
	MM3_SEED    = 538273
	ES_INDEX    = "nzb"
	esSINK_NAME = "elasticsearch"
)

func init() {
	sinks.Register(esSINK_NAME, func(config json.RawMessage) (newsrover.Sink, error) {
		var conf ElasticSinkParams
		if err := json.Unmarshal(config, &conf); err == nil {
			return NewElasticSink(conf)
		} else {
			return nil, err
		}
	})
}

type ElasticSink struct {
	articles     chan newsrover.Article
	articlesLock sync.RWMutex
	esConn       *goes.Connection
	logger       *log.Logger
	failLog      io.Writer
	failLogLock  sync.Mutex
	docBuffSize  int
	workers      int
	flushEvery   int
	processed    int64

	parentLru     *lru.Cache
	parentLruLock sync.Mutex

	host string
	port int

	stop chan bool
}

type ElasticSinkParams struct {
	Workers int       `json:"workers"`
	FailLog io.Writer `json:"-"`

	ElasticHost string `json:"host"`
	ElasticPort int    `json:"port"`
}

type Upload struct {
	Id         string         `json:"_id"`
	Poster     string         `json:"poster"`
	Subject    string         `json:"subject"`
	Date       time.Time      `json:"date"`
	Group      []string       `json:"group"`
	Dmca       bool           `json:"dmca"`
	Length     int            `json:"length"`
	Complete   int            `json:"complete"`
	Completion float64        `json:"completion"`
	Size       int64          `json:"size"`
	Segments   []*Segment     `json:"segments"`
	Types      map[string]int `json:"types"`
}

type File struct {
	Id      string    `json:"_id"`
	Poster  string    `json:"poster"`
	Subject string    `json:"subject"`
	Date    time.Time `json:"date"`
	Group   []string  `json:"group"`

	Length   int   `json:"length"`
	Complete int   `json:"complete"`
	Size     int64 `json:"size"`

	Filename string     `json:"filename"`
	Segments []*Segment `json:"segments"`

	ParentId string `json:"-"`
}

type Segment struct {
	Group           string    `json:"group"`
	Subject         string    `json:"subject,omitempty"`
	Filename        string    `json:"filename"`
	Poster          string    `json:"poster"`
	Date            time.Time `json:"date"`
	ServerArticleId int64     `json:"server_article_id"`
	MessageId       string    `json:"message_id"`
	Bytes           int64     `json:"bytes"`
	Part            int       `json:"part"`
	Length          int       `json:"length"`
	Added           time.Time `json:"added"`
}

type updateCommit struct {
	Script string      `json:"script"`
	Params interface{} `json:"params"`
	Lang   string      `json:"lang"`
	Upsert interface{} `json:"upsert,omitempty"`
}

func createSegment(article newsrover.Article) Segment {
	return Segment{
		Group:           article.Group,
		Subject:         article.Subject,
		Filename:        extract.ExtractFile(article.Subject),
		Poster:          article.From,
		Date:            article.Time(),
		ServerArticleId: int64(article.ArticleId),
		MessageId:       article.MessageId,
		Bytes:           article.Bytes,
		Length:          extract.ExtractYencLength(article.Subject),
		Part:            extract.ExtractYencPart(article.Subject),
		Added:           time.Now(),
	}
}

func createFile(article newsrover.Article) File {
	return File{
		Id:      articleFileUploadId(article),
		Poster:  article.From,
		Subject: article.Subject,
		Date:    article.Time(),
		Group:   []string{article.Group},

		Length:   extract.ExtractYencLength(article.Subject),
		Complete: 0,
		Filename: extract.ExtractFile(article.Subject),

		Segments: make([]*Segment, 0, 16),
	}
}

func createUpload(article newsrover.Article) Upload {
	return Upload{
		Id:       articleUploadId(article),
		Poster:   article.From,
		Subject:  article.Subject,
		Date:     article.Time(),
		Group:    []string{article.Group},
		Dmca:     false,
		Length:   extract.ExtractYencLength(article.Subject),
		Complete: 0,
		Size:     0,
		Segments: make([]*Segment, 0, 16),
	}
}

func fileUpdateCommit(f File) updateCommit {
	return updateCommit{
		Script: "roversinkscript",
		Lang:   "native",
		Params: struct {
			File
			Type string `json:"type"`
		}{
			File: f,
			Type: "file",
		},
		Upsert: struct {
			placeholder bool `json:"-"`
		}{true},
	}
}

func uploadUpdateCommit(u Upload) updateCommit {
	return updateCommit{
		Script: "roversinkscript",
		Lang:   "native",
		Params: struct {
			Upload
			Type string `json:"type"`
		}{
			Upload: u,
			Type:   "upload",
		},
		Upsert: struct {
			Dmca bool `json:"dmca"`
		}{false},
	}
}

func articleUploadId(article newsrover.Article) string {
	h128 := murmur3.New64(MM3_SEED)
	h128.Write([]byte(article.From))
	h128.Write([]byte(extract.ExtractRelease(article.Group, article.Subject)))
	return hex.EncodeToString(h128.Sum(nil))
}

func articleFileUploadId(article newsrover.Article) string {
	h128 := murmur3.New64(MM3_SEED)
	h128.Write([]byte(article.From))
	h128.Write([]byte(extract.ExtractRelease(article.Group, article.Subject)))
	h128.Write([]byte(extract.ExtractFile(article.Subject)))
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(extract.ExtractYencLength(article.Subject)))
	h128.Write(b)
	return hex.EncodeToString(h128.Sum(nil))
}

func (es *ElasticSink) Accept(articles []newsrover.Article) {
	// Not really sure of the performance hit of this lock here
	// (Prevents send on nil channel if sink isn't serving...)
	es.articlesLock.RLock()
	defer es.articlesLock.RUnlock()
	if es.articles == nil {
		es.logger.Println("Recieved accept when uninitialized.")
		i := make([]interface{}, len(articles))
		for idx, a := range articles {
			i[idx] = a
		}
		es.FailAll(i)
		return
	}
	g := int64(0)
	for i, a := range articles {
		if strings.Contains(strings.ToLower(a.Subject), "yenc") {
			if extract.ExtractRelease(a.Group, a.Subject) != "" {
				es.articles <- a
			}
		}
		if i%50 == 0 {
			atomic.AddInt64(&es.processed, 50)
			g += 50
		}
	}
	atomic.AddInt64(&es.processed, int64(len(articles))-g)
}

func NewElasticSink(params ElasticSinkParams) (*ElasticSink, error) {
	es := &ElasticSink{}
	es.logger = log.New(ioutil.Discard, "", log.LstdFlags)
	es.parentLru = lru.New(lruSize)
	es.docBuffSize = 4096
	es.workers = 1
	es.failLog = ioutil.Discard
	es.host = "localhost"
	es.port = 9200
	es.flushEvery = 90
	es.articles = nil
	if params.Workers > 0 {
		es.workers = params.Workers
	}
	if params.FailLog != nil {
		es.failLog = params.FailLog
	}
	if params.ElasticHost != "" {
		es.host = params.ElasticHost
	}
	if params.ElasticPort > 0 {
		es.port = params.ElasticPort
	}
	return es, nil
}

func (es *ElasticSink) Name() string {
	return esSINK_NAME
}

func (es *ElasticSink) SetLogger(logger *log.Logger) {
	if logger == nil {
		es.logger = log.New(ioutil.Discard, "", log.LstdFlags)
	} else {
		es.logger = logger
		if es.logger.Prefix() == "" {
			es.logger.SetPrefix("[ElasticSink]")
		}
	}
}

func (es *ElasticSink) Fail(article newsrover.Article) {
	es.FailAll([]interface{}{article})
}

func (es *ElasticSink) FailAll(articles []interface{}) {
	es.failLogLock.Lock()
	enc := json.NewEncoder(es.failLog)
	for _, a := range articles {
		enc.Encode(a)
		es.failLog.Write([]byte{'\n'})
	}
	es.failLogLock.Unlock()
}

func (es *ElasticSink) serve(stop <-chan bool) {
	flushTime := time.Duration(es.flushEvery) * time.Second
	flush := time.NewTimer(flushTime)
	bfSz := es.docBuffSize
	if bfSz < 0 {
		bfSz = 64
	}
	articleCount := 0
	uploadBuffer := make(map[string]Upload)
	fileBuffer := make(map[string]File)
	segmentBuffer := make([]goes.Document, 0, bfSz+1)
	flushQueue := make(chan []goes.Document, 1)
	errorFile, _ := os.OpenFile("eserrors.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	go func() {
		for {
			select {
			case docs := <-flushQueue:
				if docs != nil {
					es.logger.Printf("Indexing and updating %d documents.", len(docs))
					if r, err := es.esConn.BulkSend(ES_INDEX, docs); err != nil {
						es.logger.Printf("Error: Failed to bulk flush %d articles. %s", len(docs), err.Error())
						i := make([]interface{}, len(docs))
						for idx, d := range docs {
							i[idx] = d
						}
						es.FailAll(i)
					} else {
						es.logger.Printf("Flushed %d documents took %dms. (%d)", len(docs), r.Took, es.processed)
						if r.Errors {
							errorFile.Write(r.Items)
							errorFile.WriteString("\n")
						}
					}
				} else {
					return
				}
			}
		}
	}()

	flushDocuments := func() {
		/*
		 * The purpose of the LRU is to create documents that will then be
		 * properly updated by the RoverSinkScript that exists on the
		 * ElasticSearch Server. We decide to "create" the documents
		 * if we aren't sure they exist (we keep an LRU of created documents).
		 *
		 * The only issue is the entire process isn't threadsafe and this
		 * is what limits us to have only one worker in the sink.
		 *
		 * A race issue occurs if 1 worker buffers the create command
		 * and before it is sent to ElasticSearch, another worker
		 * issues an update command on that same document to be created.
		 *
		 * This causes the update command to fail, and we lose data.
		 */
		if len(segmentBuffer) > 0 {
			createParentDocs := make([]goes.Document, 0, len(uploadBuffer)+len(fileBuffer))
			docs := make([]goes.Document, len(uploadBuffer)+len(fileBuffer)+len(segmentBuffer))
			z := 0
			es.parentLruLock.Lock()
			for _, v := range uploadBuffer {
				if _, ok := es.parentLru.Get(v.Id); !ok {
					createParentDocs = append(createParentDocs, goes.Document{
						Index:       ES_INDEX,
						Id:          v.Id,
						Type:        "upload",
						BulkCommand: "create",
						Fields: struct {
							Dmca bool `json:"dmca"`
						}{false},
					})
					es.parentLru.Add(v.Id, true)
				}
				docs[z] = goes.Document{
					Index:       ES_INDEX,
					Type:        "upload",
					Id:          v.Id,
					BulkCommand: "update",
					Fields:      uploadUpdateCommit(v),
				}
				z++
			}
			for _, v := range fileBuffer {
				if _, ok := es.parentLru.Get(v.Id); !ok {
					createParentDocs = append(createParentDocs, goes.Document{
						Index:       ES_INDEX,
						Id:          v.Id,
						Type:        "file",
						BulkCommand: "create",
						Parent:      v.ParentId,
						Fields: struct {
							placeholder bool `json:"-"`
						}{true},
					})
					es.parentLru.Add(v.Id, true)
				}
				docs[z] = goes.Document{
					Index:       ES_INDEX,
					Id:          v.Id,
					Type:        "file",
					BulkCommand: "update",
					Fields:      fileUpdateCommit(v),
					Parent:      v.ParentId,
				}
				z++
			}
			es.parentLruLock.Unlock()
			for _, v := range segmentBuffer {
				docs[z] = v
				z++
			}
			if len(createParentDocs) > 0 {
				flushQueue <- createParentDocs
			}
			flushQueue <- docs
			for k := range uploadBuffer {
				delete(uploadBuffer, k)
			}
			for k := range fileBuffer {
				delete(fileBuffer, k)
			}
			segmentBuffer = segmentBuffer[:0]
			articleCount = 0
		}
	}

	for {
		select {
		case <-stop:
			flushDocuments()
			flush.Stop()
			flushQueue <- nil
			return
		case <-flush.C:
			flushDocuments()
			flush.Reset(flushTime)
		case article, ok := <-es.articles:
			if ok {
				articleCount++
				uploadId := articleUploadId(article)
				fileUploadId := articleFileUploadId(article)

				segment := new(Segment)
				*segment = createSegment(article)

				segmentDoc := goes.Document{
					Index:       ES_INDEX,
					Id:          segment.MessageId,
					Type:        "segment",
					BulkCommand: "create",
					Fields:      *segment,
					Parent:      fileUploadId,
				}
				segmentBuffer = append(segmentBuffer, segmentDoc)
				segment.Subject = ""
				if segmentFile, ok := fileBuffer[fileUploadId]; ok {
					segmentFile.Segments = append(segmentFile.Segments, segment)
					ad := true
					for _, g := range segmentFile.Group {
						if article.Group == g {
							ad = false
						}
					}
					if ad {
						segmentFile.Group = append(segmentFile.Group, article.Group)
					}
					segmentFile.ParentId = uploadId
					fileBuffer[fileUploadId] = segmentFile
				} else {
					segmentFile = createFile(article)
					segmentFile.ParentId = uploadId
					segmentFile.Segments = append(segmentFile.Segments, segment)
					fileBuffer[fileUploadId] = segmentFile
				}

				if segmentUpload, ok := uploadBuffer[uploadId]; ok {
					segmentUpload.Segments = append(segmentUpload.Segments, segment)
					ad := true
					for _, g := range segmentUpload.Group {
						if article.Group == g {
							ad = false
						}
					}
					if ad {
						segmentUpload.Group = append(segmentUpload.Group, article.Group)
					}
					uploadBuffer[uploadId] = segmentUpload
				} else {
					segmentUpload = createUpload(article)
					segmentUpload.Segments = append(segmentUpload.Segments, segment)
					uploadBuffer[uploadId] = segmentUpload
				}

				if es.docBuffSize > 0 {
					if articleCount >= bfSz {
						flushDocuments()
					}
				}
			}
		}
	}
}

func (es *ElasticSink) Serve() {
	es.logger.Printf("Starting ElasticSink, writing data to http://%s:%d", es.host, es.port)
	es.esConn = goes.NewConnection(es.host, es.port)
	es.articles = make(chan newsrover.Article)
	es.stop = make(chan bool)
	control := make(chan bool)
	var wg sync.WaitGroup
	for i := 0; i < es.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			es.serve(control)
		}()
	}
	select {
	case <-es.stop:
		es.articlesLock.Lock()
		close(es.articles)
		es.articles = nil
		es.articlesLock.Unlock()
		close(control)
		es.stop = nil
	}
	wg.Wait()
	es.esConn = nil
}

func (es *ElasticSink) Stop() {
	if es.stop != nil {
		es.stop <- true
	}
}
