package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

// TCPInput used for internal communication
type TCPInput struct {
	data     chan *Message
	listener net.Listener
	address  string
	config   *TCPInputConfig
	stop     chan bool // Channel used only to indicate goroutine should shutdown
}

// TCPInputConfig represents configuration of a TCP input plugin
type TCPInputConfig struct {
	Secure           bool          `json:"input-tcp-secure"`
	RewriteTimeStamp bool          `json:"input-tcp-rewrite-time-stamp"`
	CertificatePath  string        `json:"input-tcp-certificate"`
	KeyPath          string        `json:"input-tcp-certificate-key"`
	Timeout          time.Duration `json:"input-tcp-timeout"`
	BlockIPs         MultiOption   `json:"input-tcp-block-ip"`
}

// NewTCPInput constructor for TCPInput, accepts address with port
func NewTCPInput(address string, config *TCPInputConfig) (i *TCPInput) {
	i = new(TCPInput)
	i.data = make(chan *Message, 1000)
	i.address = address
	i.config = config
	i.stop = make(chan bool)

	i.listen(address)

	return
}

// PluginRead returns data and details read from plugin
func (i *TCPInput) PluginRead() (msg *Message, err error) {
	select {
	case <-i.stop:
		return nil, ErrorStopped
	case msg = <-i.data:
		return msg, nil
	}

}

// Close closes the plugin
func (i *TCPInput) Close() error {
	close(i.stop)
	i.listener.Close()
	return nil
}

func (i *TCPInput) listen(address string) {
	if i.config.Secure {
		cer, err := tls.LoadX509KeyPair(i.config.CertificatePath, i.config.KeyPath)
		if err != nil {
			log.Fatalln("error while loading --input-tcp TLS certificate:", err)
		}

		config := &tls.Config{Certificates: []tls.Certificate{cer}}
		listener, err := tls.Listen("tcp", address, config)
		if err != nil {
			log.Fatalln("[INPUT-TCP] failed to start INPUT-TCP listener:", err)
		}
		i.listener = listener
	} else {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			log.Fatalln("failed to start INPUT-TCP listener:", err)
		}
		i.listener = listener
	}
	go func() {
		for {
			conn, err := i.listener.Accept()
			if err == nil {
				go i.handleConnection(conn)
				continue
			}
			if isTemporaryNetworkError(err) {
				continue
			}
			if operr, ok := err.(*net.OpError); ok && operr.Err.Error() != "use of closed network connection" {
				Debug(0, fmt.Sprintf("[INPUT-TCP] listener closed, err: %q", err))
			}
			break
		}
	}()
}

var payloadSeparatorAsBytes = []byte(payloadSeparator)

// copyBuffer alloc & copy buffer
func copyBuffer(buf []byte) []byte {
	new_buf := make([]byte, len(buf))
	copy(new_buf, buf)
	return new_buf
}

// rewriteTimeStamp rewrite timestamp in meta to current time
func rewriteTimeStamp(buf []byte) []byte {
	meta := payloadMeta(buf)

	if len(meta) > 3 {
		meta[2] = []byte(fmt.Sprintf("%d", time.Now().UnixNano()))

		return append(bytes.Join(meta, []byte(" ")), '\n')
	}

	return copyBuffer(buf)
}

func (i *TCPInput) handleConnection(conn net.Conn) {
	defer conn.Close()

	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err == nil {
		for _, ip := range i.config.BlockIPs {
			if ip == host {
				// discard this connection
				fmt.Fprintln(os.Stderr, "block connection from", conn.RemoteAddr())
				time.Sleep(time.Second)
				return
			}
		}
	}

	reader := bufio.NewReader(conn)
	var buffer bytes.Buffer

	lastUpdateTime := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if isTemporaryNetworkError(err) {
				continue
			}
			if err != io.EOF {
				Debug(0, fmt.Sprintf("[INPUT-TCP] connection error: %q", err))
			}
			break
		}

		if bytes.Equal(payloadSeparatorAsBytes[1:], line) {
			// unread the '\n' before monkeys
			buffer.UnreadByte()
			var msg Message
			meta, data := payloadMetaWithBody(buffer.Bytes())
			msg.Meta = copyBuffer(meta)
			msg.Data = copyBuffer(data)

			// Fix inconsistent timestamps of different machines.
			if i.config.RewriteTimeStamp {
				msg.Meta = rewriteTimeStamp(msg.Meta)
			}

		F:
			for {
				select {
				case i.data <- &msg:
					lastUpdateTime = time.Now()
					break F

				case <-ticker.C:
					expiredTime := lastUpdateTime.Add(i.config.Timeout)
					if time.Now().After(expiredTime) {
						return
					}
				}
			}
			buffer.Reset()
		} else {
			buffer.Write(line)
		}
	}
}

func (i *TCPInput) String() string {
	return "TCP input: " + i.address
}

func isTemporaryNetworkError(err error) bool {
	if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		return true
	}
	if operr, ok := err.(*net.OpError); ok && operr.Temporary() {
		return true
	}
	return false
}
