package demo1

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func SendMsg() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	msg := &sarama.ProducerMessage{
		Topic: "test_log",
		Value: sarama.StringEncoder("this is a test log"),
	}
	client, err := sarama.NewSyncProducer([]string{"47.104.182.184:9092"}, config)
	if err != nil {
		fmt.Println("send msg failed, err:", err)
		return
	}
	defer client.Close()

	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("send msg failed, err:", err)
		return
	}

	fmt.Printf("pid: %v offset:%v\n", pid, offset)
}
