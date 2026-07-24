package main

import (
	"bufio"
	"client/internal"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	fmt.Println("Enter filename: ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if err := internal.UploadFile(input); err != nil {
		log.Panic(err)
	}
}
