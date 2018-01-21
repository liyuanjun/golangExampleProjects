package main

import (
	"github.com/amlyj/golangExampleProjects/pipeline/pipeline"
	"fmt"
)

func main() {
	p := pipeline.ArraySource(2, 3, 4, 5, 6, 1, 3, 6)

	for {
		// 从ArraySource的channel中取数据
		if num, ok := <-p; ok {
			fmt.Println(num)
		} else {
			break
		}
	}

}
