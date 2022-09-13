package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type MessageModel struct {
	ID      int
	Header  string
	Message string
	Date    time.Time
}

func main() {

	var message, header string
	var count int
	flag.StringVar(&header, "header", "", "write your header")
	flag.StringVar(&message, "message", "", "write your message")
	flag.IntVar(&count, "count", 0, "write your count")
	flag.Parse()

	ctx := context.Background()

	producer(ctx, header, message, count)
}

func producer(ctx context.Context, header string, message string, count int) {
	fmt.Println("producer started")

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}

	for i := 1; i <= count; i++ {

		model := MessageModel{
			ID:      i,
			Header:  header + strconv.Itoa(i),
			Message: message + strconv.Itoa(i),
			Date:    time.Now(),
		}

		msg, err := json.Marshal(model)

		if err != nil {
			fmt.Println("An error is occured: ", err)
			continue
		}

		//msg := message + " " + strconv.Itoa(i)
		err2 := writer.WriteMessages(ctx,
			kafka.Message{
				Value: []byte(msg),
			},
		)

		if err2 != nil {
			fmt.Println("An error is occured: ", err)
			continue
		}

		fmt.Println("Message sent.")
	}
}
