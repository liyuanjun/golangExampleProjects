package pipeline

import (
	"os"
	"bufio"
)

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
