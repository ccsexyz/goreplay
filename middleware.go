package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

// Middleware represents a middleware object
type Middleware struct {
	command       string
	data          chan *Message
	Stdin         io.Writer
	Stdout        io.Reader
	commandCancel context.CancelFunc
	stop          chan bool // Channel used only to indicate goroutine should shutdown
}

// NewMiddleware returns new middleware
func NewMiddleware(command string) *Middleware {
	m := new(Middleware)
	m.command = command
	m.data = make(chan *Message, 1000)
	m.stop = make(chan bool)

	commands := strings.Split(command, " ")
	ctx, cancl := context.WithCancel(context.Background())
	m.commandCancel = cancl
	cmd := exec.CommandContext(ctx, commands[0], commands[1:]...)

	m.Stdout, _ = cmd.StdoutPipe()
	m.Stdin, _ = cmd.StdinPipe()

	cmd.Stderr = os.Stderr

	go m.read(m.Stdout)

	go func() {
		err := cmd.Start()

		if err != nil {
			log.Fatal(err)
		}

		err = cmd.Wait()

		if err != nil {
			log.Fatal(err)
		} else {
			// fix: program hangs after Middleware crashes
			log.Fatal("middleware unexpectedly exit!")
		}
	}()

	return m
}

// ReadFrom start a worker to read from this plugin
func (m *Middleware) ReadFrom(plugin PluginReader) {
	Debug(2, "[MIDDLEWARE-MASTER] Starting reading from", plugin)
	go m.copy(m.Stdin, plugin)
}

func (m *Middleware) copy(to io.Writer, from PluginReader) {
	for {
		msg, err := from.PluginRead()
		if err != nil {
			return
		}
		if msg == nil || len(msg.Data) == 0 {
			continue
		}

		data := msg.Data
		if Settings.PrettifyHTTP {
			data = prettifyHTTP(data)
		}

		buf := make([]byte, len(msg.Meta)+len(data))
		copy(buf, msg.Meta)
		copy(buf[len(msg.Meta):], data)

		dst := make([]byte, len(buf)*2+1)
		hex.Encode(dst, buf)
		dst[len(buf)*2] = '\n'

		to.Write(dst)

	}
}

func (m *Middleware) read(from io.Reader) {
	reader := bufio.NewReader(from)
	var line []byte
	var e error

	for {
		if line, e = reader.ReadBytes('\n'); e != nil {
			if e == io.EOF {
				continue
			} else {
				break
			}
		}

		if len(line) < 4 {
			log.Println("[MIDDLEWARE] invalid line, len =", len(line), "line =", line)
			continue
		}

		buf := make([]byte, len(line)/2)
		if _, err := hex.Decode(buf, line[:len(line)-1]); err != nil {
			Debug(0, fmt.Sprintf("[MIDDLEWARE] failed to decode err: %q", err))
			continue
		}
		var msg Message
		msg.Meta, msg.Data = payloadMetaWithBody(buf)
		select {
		case <-m.stop:
			return
		case m.data <- &msg:
		}
	}

	return
}

// PluginRead reads message from this plugin
func (m *Middleware) PluginRead() (msg *Message, err error) {
	select {
	case <-m.stop:
		return nil, ErrorStopped
	case msg = <-m.data:
	}

	return
}

func (m *Middleware) String() string {
	return fmt.Sprintf("Modifying traffic using %q command", m.command)
}

// Close closes this plugin
func (m *Middleware) Close() error {
	m.commandCancel()
	close(m.stop)
	return nil
}
