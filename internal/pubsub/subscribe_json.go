package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // Bitch another kind of enum
	handler func(T),
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
			handler(target)
			err := delivery.Ack(false)
			if err != nil {
				return
			}
		}
	}()
	return nil // nothing happens were good lol
}
