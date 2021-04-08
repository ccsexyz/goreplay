package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync"
	"time"

	"github.com/ccsexyz/goreplay/byteutils"
)

// Emitter represents an abject to manage plugins communication
type Emitter struct {
	sync.WaitGroup
	plugins *InOutPlugins
}

// NewEmitter creates and initializes new Emitter object.
func NewEmitter() *Emitter {
	return &Emitter{}
}

// Start initialize loop for sending data from inputs to outputs
func (e *Emitter) Start(plugins *InOutPlugins, middlewareCmds []string) {
	if Settings.CopyBufferSize < 1 {
		Settings.CopyBufferSize = 5 << 20
	}
	e.plugins = plugins

	if len(middlewareCmds) > 0 {
		middlewares := make([]*Middleware, 0, len(middlewareCmds))
		for _, cmd := range middlewareCmds {
			middleware := NewMiddleware(cmd)
			middlewares = append(middlewares, middleware)
		}

		first := middlewares[0]

		for _, in := range plugins.Inputs {
			first.ReadFrom(in)
		}

		for it := 1; it < len(middlewares); it++ {
			middlewares[it].ReadFrom(middlewares[it-1])
		}

		last := middlewares[len(middlewares)-1]

		go func() {
			defer e.Done()
			if err := CopyMulty(last, plugins.Outputs...); err != nil {
				Debug(2, fmt.Sprintf("[EMITTER] error during copy: %q", err))
			}
		}()

		for _, middleware := range middlewares {
			e.plugins.Inputs = append(e.plugins.Inputs, middleware)
			e.plugins.All = append(e.plugins.All, middleware)
			e.Add(1)
		}
	} else {
		for _, in := range plugins.Inputs {
			e.Add(1)
			go func(in PluginReader) {
				defer e.Done()
				if err := CopyMulty(in, plugins.Outputs...); err != nil {
					Debug(2, fmt.Sprintf("[EMITTER] error during copy: %q", err))
				}
			}(in)
		}
	}
}

// Close closes all the goroutine and waits for it to finish.
func (e *Emitter) Close() {
	for _, p := range e.plugins.All {
		if cp, ok := p.(io.Closer); ok {
			cp.Close()
		}
	}
	if len(e.plugins.All) > 0 {
		// wait for everything to stop
		e.Wait()
	}
	e.plugins.All = nil // avoid Close to make changes again
}

type simpleWriter struct {
	w  PluginWriter
	ch chan *Message
}

func newSimpleWriter(w PluginWriter) *simpleWriter {
	s := &simpleWriter{
		w:  w,
		ch: make(chan *Message, 65536),
	}
	go s.writer()
	return s
}

func (s *simpleWriter) writer() {
	for {
		msg, ok := <-s.ch
		if !ok {
			break
		}
		s.w.PluginWrite(msg)
	}
}

func (s *simpleWriter) PluginWrite(msg *Message) (n int, err error) {
	select {
	default:
	case s.ch <- msg:
	}
	return len(msg.Data), nil
}

// CopyMulty copies from 1 reader to multiple writers
func CopyMulty(src PluginReader, orig_writers ...PluginWriter) error {
	wIndex := 0
	modifier := NewHTTPModifier(&Settings.ModifierConfig)
	filteredRequests := make(map[string]int64)
	filteredRequestsLastCleanTime := time.Now().UnixNano()
	filteredCount := 0

	var writers []PluginWriter
	for it := 0; it < len(orig_writers); it++ {
		if len(orig_writers) > 1 {
			writers = append(writers, newSimpleWriter(orig_writers[it]))
		} else {
			// if only have 1 dest writer, no need to make simple writer
			writers = append(writers, orig_writers[it])
		}
	}

	for {
		msg, err := src.PluginRead()
		if err != nil {
			if err == ErrorStopped || err == io.EOF {
				return nil
			}
			return err
		}
		if msg != nil && len(msg.Data) > 0 {
			if len(msg.Data) > int(Settings.CopyBufferSize) {
				msg.Data = msg.Data[:Settings.CopyBufferSize]
			}
			meta := payloadMeta(msg.Meta)
			if len(meta) < 3 {
				Debug(2, fmt.Sprintf("[EMITTER] Found malformed record %q from %q", msg.Meta, src))
				continue
			}
			requestID := byteutils.SliceToString(meta[1])
			// start a subroutine only when necessary
			if Settings.Verbose >= 3 {
				Debug(3, "[EMITTER] input: ", byteutils.SliceToString(msg.Meta[:len(msg.Meta)-1]), " from: ", src)
			}
			if modifier != nil {
				Debug(3, "[EMITTER] modifier:", requestID, "from:", src)
				if isRequestPayload(msg.Meta) {
					msg.Data = modifier.Rewrite(msg.Data)
					// If modifier tells to skip request
					if len(msg.Data) == 0 {
						filteredRequests[requestID] = time.Now().UnixNano()
						filteredCount++
						continue
					}
					Debug(3, "[EMITTER] Rewritten input:", requestID, "from:", src)

				} else {
					if _, ok := filteredRequests[requestID]; ok {
						delete(filteredRequests, requestID)
						filteredCount--
						continue
					}
				}
			}

			if Settings.PrettifyHTTP {
				msg.Data = prettifyHTTP(msg.Data)
				if len(msg.Data) == 0 {
					continue
				}
			}

			if Settings.SplitOutput {
				if Settings.RecognizeTCPSessions {
					if !PRO {
						log.Fatal("Detailed TCP sessions work only with PRO license")
					}
					hasher := fnv.New32a()
					hasher.Write(meta[1])

					wIndex = int(hasher.Sum32()) % len(writers)
					if _, err := writers[wIndex].PluginWrite(msg); err != nil {
						return err
					}
				} else {
					// Simple round robin
					if _, err := writers[wIndex].PluginWrite(msg); err != nil {
						return err
					}

					wIndex = (wIndex + 1) % len(writers)
				}
			} else {
				for _, dst := range writers {
					if _, err := dst.PluginWrite(msg); err != nil {
						return err
					}
				}
			}
		}

		// Run GC on each 1000 request
		if filteredCount > 0 && filteredCount%1000 == 0 {
			// Clean up filtered requests for which we didn't get a response to filter
			now := time.Now().UnixNano()
			if now-filteredRequestsLastCleanTime > int64(60*time.Second) {
				for k, v := range filteredRequests {
					if now-v > int64(60*time.Second) {
						delete(filteredRequests, k)
						filteredCount--
					}
				}
				filteredRequestsLastCleanTime = time.Now().UnixNano()
			}
		}
	}
}
