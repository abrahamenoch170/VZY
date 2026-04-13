package main

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "server address")
	clients := flag.Int("clients", 1000, "number of clients")
	messages := flag.Int("messages", 20, "messages per client")
	flag.Parse()

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := dial(*addr, fmt.Sprintf("/ws?roomId=stress&clientId=%d", id))
			if err != nil {
				return
			}
			defer c.Close()
			for m := 0; m < *messages; m++ {
				payload := []byte(fmt.Sprintf(`{"event":"operation","data":{"type":"SET","key":"k%d","value":%d,"clientId":"%d","timestamp":%d,"roomId":"stress","opId":"123e4567-e89b-42d3-a456-426614174%03d"}}`, m, m, id, time.Now().UnixNano(), m%1000))
				_ = writeMaskedFrame(c, payload)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("done clients=%d messages=%d duration=%s\n", *clients, *messages, time.Since(start))
}

func dial(addr, path string) (net.Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	k := make([]byte, 16)
	_, _ = rand.Read(k)
	key := base64.StdEncoding.EncodeToString(k)
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", path, addr, key)
	_, err = c.Write([]byte(req))
	return c, err
}

func writeMaskedFrame(c net.Conn, payload []byte) error {
	mask := [4]byte{1, 2, 3, 4}
	head := []byte{0x81}
	l := len(payload)
	switch {
	case l < 126:
		head = append(head, 0x80|byte(l))
	case l <= 65535:
		head = append(head, 0x80|126, byte(l>>8), byte(l))
	default:
		head = append(head, 0x80|127)
		n := uint64(l)
		ext := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			ext[i] = byte(n & 0xFF)
			n >>= 8
		}
		head = append(head, ext...)
	}
	if _, err := c.Write(head); err != nil {
		return err
	}
	if _, err := c.Write(mask[:]); err != nil {
		return err
	}
	out := make([]byte, len(payload))
	for i := range payload {
		out[i] = payload[i] ^ mask[i%4]
	}
	_, err := c.Write(out)
	return err
}
