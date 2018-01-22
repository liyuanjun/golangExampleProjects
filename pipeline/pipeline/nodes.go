// 节点
package pipeline

import (
	"sort"
	"io"
	"encoding/binary"
	"math/rand"
)

// 将Array中的数据导入到channel
func ArraySource(data ... int) <-chan int {
	out := make(chan int)
	// 必须在go routine中使用channel
	go func() {
		for _, v := range data {
			out <- v
		}
		// 关闭channel，表示传输结束
		close(out)
	}()
	return out
}

// 取出channel数据在内存排序，然后返回排好序的channel
func InMemorySort(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		// 将channel中的数据读进内存
		var data []int
		for v := range in {
			data = append(data, v)
		}
		// 排序
		sort.Ints(data)
		// 输出
		for _, v := range data {
			out <- v
		}
		// 关闭channel
		close(out)
	}()
	return out
}

// 合并两个channel
func Merge2(int1, int2 <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		v1, ok1 := <-int1
		v2, ok2 := <-int2
		for ok1 || ok2 {
			// 比较获取最小值，并为其重新设置变量值
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-int1
			} else {
				out <- v2
				v2, ok2 = <-int2
			}
		}
		// 关闭channel
		close(out)
	}()
	return out
}

// 读取文件作为数据源
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int)
	go func() {
		// 声明一个8字节大小的buffer对象
		buffer := make([]byte, 8)
		readSize := 0
		for {
			n, err := reader.Read(buffer)
			readSize += n
			// EOF结尾 或者 读取数量大于chunkSize即退出
			if err != nil || (chunkSize != -1 && readSize > chunkSize) {
				break
			}
			if n > 0 {
				// 从buff中获取数据
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
		}
		close(out)
	}()
	return out
}

// 将channel数据写入文件
func WriterSink(writer io.Writer, in <-chan int) {
	for {
		buffer := make([]byte, 8)
		// 从channel中取数据
		if data, ok := <-in; ok {
			binary.BigEndian.PutUint64(buffer, uint64(data))
			writer.Write(buffer)
		} else {
			break
		}
	}
}

// 随机int 数据源
func RandomIntSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

// 合并多个channel
func MergeN(inputs ... <-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	middleIndex := len(inputs) / 2
	// 递归 merge inputs[0:middleIndex] and merge inputs[middleIndex:len-1]
	return Merge2(MergeN(inputs[:middleIndex]...), MergeN(inputs[middleIndex:]...))
}
