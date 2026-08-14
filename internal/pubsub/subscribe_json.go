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
	queueType SimpleQueueType,
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
				err := delivery.Ack(false)
				if err != nil {
					return
				}
				log.Println("Acked message")
			case NackDiscard:
				err := delivery.Nack(false, false)
				if err != nil {
					return
				}
				log.Println("NackDiscard: message discarded")
			case NackRequeue:
				err := delivery.Nack(false, true)
				if err != nil {
					return
				}
				log.Println("NackRequeue: message requeued")
			}
		}
	}()
	return nil // nothing happens were good lol
}
