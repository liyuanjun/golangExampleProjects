package main

import (
	"github.com/amlyj/golangExampleProjects/pipeline/pipeline"
	"fmt"
	"os"
	"bufio"
)

func main() {
	randomFile := "file.in"
	readFIle := "file.out"

	dataSize := 64 * 1024 * 1024 // 512 MB
	fileSize := dataSize * 8 // bytes
	chunkCount := 64
	// 创建随机数字组成的文件
	createRandomIntFIle(randomFile, dataSize)
	// 构建pipeline
	p := pipeline.CreatePipeline(randomFile, fileSize, chunkCount)
	// 将归并排序后的数据写入文件
	pipeline.WriterToFIle(p, readFIle)
	// 读取排序后的文件
	file, err := os.Open(readFIle)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	sourceData := pipeline.ReaderSource(file, -1)
	for data := range sourceData {
		fmt.Println(data)
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
