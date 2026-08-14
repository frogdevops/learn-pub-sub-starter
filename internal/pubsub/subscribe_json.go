package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // Bitch another kind of enum
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	// RIP
	// DONT CLOSE THIS CHANNEL

	deliverChannel, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	go func() {
		for delivery := range deliverChannel {
			var target T
			err = json.Unmarshal(delivery.Body, &target)
			if err != nil {
				log.Printf("could not unmarshal message: %v", err)
				continue
			}
			ackType := handler(target)
			switch ackType {
			case Ack:
				delivery.Ack(false)
				log.Println("Acked message")
			case NackDiscard:
				delivery.Nack(false, false)
				log.Println("NackDiscard: message discarded")
			case NackRequeue:
				delivery.Nack(false, true)
				log.Println("NackRequeue: message requeued")
			}
		}
	}()
	return nil // nothing happens were good lol
}
