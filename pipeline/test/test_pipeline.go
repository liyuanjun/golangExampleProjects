package main

import (
	"github.com/amlyj/golangExampleProjects/pipeline/pipeline"
	"fmt"
	"os"
	"math/rand"
	"bufio"
)

func closeOff() {
	fmt.Println("")
	fmt.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	fmt.Println()
}

func main() {
	testFile("small.in", 64) // 64 * 8
	//testFile("large.in",64*1024*1024)

}

func testSort() {
	p := pipeline.ArraySource(2, 3, 4, 5, 6, 1, 3, 6)
	for {
		// 从ArraySource的channel中取数据
		if num, ok := <-p; ok {
			fmt.Print(num)
		} else {
			break
		}
	}
	closeOff()

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

func testMerge() {
	closeOff()
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

func testRand() {
	fmt.Println(rand.Int())
	fmt.Println(rand.Uint64())
	for i := 0; i < 10; i++ {
		fmt.Println(rand.Intn(50))
	}
}

func testFile(fileName string, size int) {
	closeOff()

	smallFile, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer smallFile.Close()
	// 将file对象转为buff io,使其变为缓冲流，写入速度更快
	writer := bufio.NewWriter(smallFile)
	// 生成随机数
	data := pipeline.RandomIntSource(size)
	// 写入文件
	pipeline.WriterSink(writer, data)
	// 清空缓存，避免数据不完整
	writer.Flush()

	// 读取文件数据
	smallFile, err = os.Open(fileName)
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
