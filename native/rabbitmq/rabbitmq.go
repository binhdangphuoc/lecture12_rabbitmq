package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

const(
	addressMQ = "amqp://guest:guest@localhost:5672/"
)

type ChannelMQ struct{
	Channel 		*amqp.Channel
	Name 			string
}

type MQ interface {
	PublishMQ(string) 	error
	ConsumeMQ()			(<-chan amqp.Delivery,error)
}

func NewChannelMQ(name string) (*ChannelMQ){
	conn, err := amqp.Dial(addressMQ)
	if err != nil {
		fmt.Println("Can't connect to rabbitMQ")
		return nil
	}
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Can't open channel rabbitMQ")
		return nil
	}
	q, err := ch.QueueDeclare(
		name, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		fmt.Println("Can't declare channel rabbitMQ")
		return nil
	}
	return &ChannelMQ{
		Channel: ch,
		Name: q.Name,
	}
}

func (mq *ChannelMQ) PublishMQ(str string) error {
	err := mq.Channel.Publish(
		"",     // exchange
		mq.Name, 		// routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(str),
		})
	return err
}

func (mq *ChannelMQ) ConsumeMQ() (<-chan amqp.Delivery, error){
	msgs, err := mq.Channel.Consume(
		mq.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	return msgs, err
}
