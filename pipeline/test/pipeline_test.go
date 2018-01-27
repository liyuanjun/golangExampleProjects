package main

import (
	"github.com/amlyj/golangExampleProjects/pipeline/pipeline"
	"fmt"
	"os"
	"math/rand"
	"bufio"
	"testing"
)

func TestArraySource(t *testing.T) {
	p := pipeline.ArraySource(2, 3, 4, 5, 6, 1, 3, 6)
	for {
		// 从ArraySource的channel中取数据
		if num, ok := <-p; ok {
			fmt.Print(num)
		} else {
			break
		}
	}
	fmt.Println()
	fmt.Println()
	pSort := pipeline.InMemorySort(pipeline.ArraySource(2, 3, 4, 5, 6, 1, 3, 6))
	for {
		// 从ArraySource的channel中取数据
		if num, ok := <-pSort; ok {
			fmt.Print(num)
		} else {
			break
		}
	}

}

func TestMerge2(t *testing.T) {
	merge2 := pipeline.Merge2(
		pipeline.InMemorySort(pipeline.ArraySource(5, 6, 1, 3, 6)),
		pipeline.InMemorySort(pipeline.ArraySource(2, 3, 4)))
	for {
		// 从ArraySource的channel中取数据
		if num, ok := <-merge2; ok {
			fmt.Print(num)
		} else {
			break
		}
	}
}

func TestRand(t *testing.T) {
	fmt.Println(rand.Int())
	fmt.Println(rand.Uint64())
	for i := 0; i < 10; i++ {
		fmt.Println(rand.Intn(50))
	}
}

func TestRandomIntSource(t *testing.T)  {
	fileName:="test.in"
	dataSize:= 50

	smallFile, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer smallFile.Close()
	// 将file对象转为buff io,使其变为缓冲流，写入速度更快
	writer := bufio.NewWriter(smallFile)
	// 生成随机数
	data := pipeline.RandomIntSource(dataSize)
	// 写入文件
	pipeline.WriterSink(writer, data)
	// 清空缓存，避免数据不完整
	writer.Flush()


}

func TestReaderSource(t *testing.T)  {
	fileName:="test.in"
	// 读取文件数据
	smallFile, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	// 将file对象转为buff io,使其变为缓冲流，读取速度更快
	reader := bufio.NewReader(smallFile)
	chData := pipeline.ReaderSource(reader, -1)
	for i := range chData {
		fmt.Println(i)
	}
}

func TestInMemorySort(t *testing.T)  {
	fileName:="test.in"
	// 读取文件数据
	smallFile, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	// 将file对象转为buff io,使其变为缓冲流，读取速度更快
	reader := bufio.NewReader(smallFile)
	chData := pipeline.ReaderSource(reader, -1)
	// 排序
	chData = pipeline.InMemorySort(chData)
	for i := range chData {
		fmt.Println(i)
	}
}


func TestNetworkSink(t *testing.T)  {
	addr :=":10010"
	pipeline.NetworkSink(addr,pipeline.InMemorySort(pipeline.ArraySource(5, 6, 1, 3, 6)))
	data:=pipeline.NetworkSource(addr)
	for v:= range data{
		fmt.Print(v)
	}
}

func TestNetworkPipeLine(t *testing.T)  {
	randomFile := "file_net.in"
	readFIle := "file_net.out"

	dataSize := 64 * 1024 * 1024 // 512 MB
	fileSize := dataSize * 8     // bytes
	chunkCount := 4
	createRandomIntFIle(randomFile,dataSize)
	p := pipeline.CreateNetworkPipeline(randomFile, fileSize, chunkCount)
	// 将归并排序后的数据写入文件
	pipeline.WriterToFIle(p, readFIle)
	// 读取排序后的文件
	file, err := os.Open(readFIle)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	sourceData := pipeline.ReaderSource(file, -1)

	printSize := 0
	for data := range sourceData {
		if printSize > 50 {
			break
		}
		fmt.Println(data)
		printSize += 1

	}
}


func createRandomIntFIle(fileName string, dataSize int) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 将file对象转为buff io,使其变为缓冲流，写入速度更快
	writer := bufio.NewWriter(file)
	// 生成随机数
	data := pipeline.RandomIntSource(dataSize)
	// 清空缓存，避免数据不完整
	defer writer.Flush()
	// 写入文件
	pipeline.WriterSink(writer, data)

}