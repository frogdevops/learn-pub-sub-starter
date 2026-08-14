package pubsub

import (
	"bytes"
	"encoding/gob"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return err
	}
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
			target, err := unmarshaller(delivery.Body)
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
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler,
		func(data []byte) (T, error) {
			var target T
			buf := bytes.NewBuffer(data)
			err := gob.NewDecoder(buf).Decode(&target)
			return target, err
		},
	)
}
