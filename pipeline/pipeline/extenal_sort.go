package pipeline

import (
	"os"
	"bufio"
	"strconv"
)

const pipelineStartPort  = 10010

// 根据要排序的大文件进行构建pipeline
func CreatePipeline(fileName string, fileSize, chunkCount int) <-chan int {
	Init()
	chunkSize := fileSize / chunkCount
	var sortResults []<-chan int
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		// 分块写入文件的位置
		file.Seek(int64(chunkSize*i), 0)
		reader := bufio.NewReader(file)

		source := ReaderSource(reader, chunkSize)
		sortResults = append(sortResults, InMemorySort(source))
	}
	return MergeN(sortResults...)
}

// 将排序好的文件写入排序文件
func WriterToFIle(p <-chan int, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	WriterSink(writer, p)
	defer writer.Flush() // 必须关闭
}


// 根据要排序的大文件进行构建网络pipeline
func CreateNetworkPipeline(fileName string, fileSize, chunkCount int) <-chan int {
	Init()
	chunkSize := fileSize / chunkCount
	var sortAddrs []string
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		// 分块写入文件的位置
		file.Seek(int64(chunkSize*i), 0)
		reader := bufio.NewReader(file)
		// 读取数据
		source := ReaderSource(reader, chunkSize)
		// 生成端口号
		addr := ":"+ strconv.Itoa(pipelineStartPort+i)
		// 写入网络数据
		NetworkSink(addr,InMemorySort(source))
		sortAddrs=append(sortAddrs, addr)
	}
	var sortResults []<-chan int
	for _, addr := range sortAddrs{
		// 将网络数据加载到sortResults
	   sortResults=append(sortResults, NetworkSource(addr))
	}
	return MergeN(sortResults...)
}