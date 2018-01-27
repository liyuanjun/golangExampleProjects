package pipeline

import (
	"net"
	"bufio"
)

const network = "tcp"

// 网络版写数据
func NetworkSink(addr string, in <-chan int) {
	listener, err := net.Listen(network, addr)
	if err != nil {
		panic(err)
	}
	// 使用go func 等待网络数据传输
	go func() {
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		writer := bufio.NewWriter(conn)
		defer writer.Flush()
		// 调用本地WriterSink方法写数据
		WriterSink(writer, in)
	}()
}

func NetworkSource(addr string) <-chan int {
	out := make(chan int)
	go func() {
		conn, err := net.Dial(network, addr)
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		netSource := ReaderSource(reader, -1)
		for v := range netSource {
			out <- v
		}
		close(out)
	}()
	return out
}
