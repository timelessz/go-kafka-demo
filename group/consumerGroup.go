package group

import (
	"context"
	"fmt"
	sarama "github.com/Shopify/sarama"
)

type consumerGroupHandler struct {
	name string
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	i := 0
	for msg := range claim.Messages() {
		fmt.Println(msg.Offset)
		fmt.Println(msg.Value)
		fmt.Println(msg.Key)
		sess.MarkMessage(msg, "")
		i++
		if i%15 == 0 {
			sess.Commit()
		}
	}
	return nil
}

func ConsumerGroup() {
	fmt.Println("consumer_test")
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false
	config.Version = sarama.V3_1_0_0
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	group, err := sarama.NewConsumerGroup([]string{"47.104.182.184:9092"}, "t", config)
	//sarama.NewConsumerGroupFromClient()
	if err != nil {
		panic(err)
	}
	defer group.Close()
	for {
		handler := consumerGroupHandler{name: "autocommit_test"}
		err := group.Consume(context.Background(), []string{"web_log"}, handler)
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}
