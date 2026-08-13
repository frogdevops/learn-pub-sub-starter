package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}

	defer func(conn *amqp091.Connection) {
		err := conn.Close()
		if err != nil {
			log.Fatal("Closing went wrong")
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create a channel: %v", err)
	}

	defer func(ch *amqp091.Channel) {
		err := ch.Close()
		if err != nil {
			log.Fatal("Closing went wrong")
		}
	}(ch)

	exchange := routing.ExchangePerilDirect
	key := routing.PauseKey
	gamelogic.PrintServerHelp()

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueType(0),
	)
	if err != nil {
		log.Fatalf("could not declare game_logs queue: %v", err)
	}

outer:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message")
			err := pubsub.PublishJSON(ch, exchange, key, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Printf("could not publish pause message: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message")
			err := pubsub.PublishJSON(ch, exchange, key, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Printf("could not publish resume message: %v", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			break outer
		default:
			fmt.Println("Command not recognized")
		}
	}
}
