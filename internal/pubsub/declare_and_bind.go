package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	direct SimpleQueueType = iota
	transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	var durable bool
	var autoDelete bool
	var exclusive bool
	switch queueType {
	case direct:
		durable = true
		autoDelete = false
		exclusive = false

	case transient:
		durable = false
		autoDelete = true
		exclusive = true
	default:
		return nil, amqp.Queue{}, fmt.Errorf("unknown queue type: %v", queueType)
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	if err := ch.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, queue, nil
}
