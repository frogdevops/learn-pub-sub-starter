package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}

	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Fatal("Closing went wrong")
		}
	}(conn)

	user, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("%s", err)
	}
	pause := routing.PauseKey
	exchange := routing.ExchangePerilDirect
	queueName := fmt.Sprintf("%s.%s", pause, user)

	ch, queue, err := pubsub.DeclareAndBind(conn, exchange, queueName, pause, pubsub.SimpleQueueType(1))

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Fatal("Closing went wrong")
		}
	}(ch)

	fmt.Println("Queue declared and bound:", queue.Name)

	gameState := gamelogic.NewGameState(user)
outer:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Println(err)
			}
		case "move":
			army, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("move %s\n", army.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			break outer
		default:
			fmt.Println("Not recognized")
		}

	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
