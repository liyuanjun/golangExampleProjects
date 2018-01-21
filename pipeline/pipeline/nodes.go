// 节点
package pipeline

// 将Array中的数据导入到channel
func ArraySource(data ... int) chan int {
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
