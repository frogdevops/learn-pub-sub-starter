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

func handlerPause(gs *gamelogic.GameState) func(state routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
}

func handlerMove(gs *gamelogic.GameState) func(move gamelogic.ArmyMove) {
	return func(state gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(state)
	}
}
func main() {
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	defer publishCh.Close()
	user, err := gamelogic.ClientWelcome()

	if err != nil {
		log.Fatalf("%s", err)
	}
	gameState := gamelogic.NewGameState(user)

	pauseExchange := routing.ExchangePerilDirect
	pauseKey := routing.PauseKey
	pauseQueueName := fmt.Sprintf("%s.%s", pauseKey, user)
	if err := pubsub.SubscribeJSON(
		conn,
		pauseExchange,
		pauseQueueName,
		pauseKey,
		pubsub.SimpleQueueType(1), // transient
		handlerPause(gameState),
	); err != nil {
		log.Fatalf("could not subscribe to pause queue: %v", err)
	}

	moveExchange := routing.ExchangePerilTopic
	moveBindingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	moveQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, user)
	if err := pubsub.SubscribeJSON(
		conn,
		moveExchange,
		moveQueueName,
		moveBindingKey,
		pubsub.SimpleQueueType(1), // transient
		handlerMove(gameState),
	); err != nil {
		log.Fatalf("could not subscribe to army moves queue: %v", err)
	}
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Fatal("Closing went wrong")
		}
	}(conn) //WE CLOSE IT HERE

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
				continue
			}
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", user), army)
			if err != nil {
				log.Printf("could not publish move: %v", err)
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
