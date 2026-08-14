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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(state gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(state)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{
					Attacker: state.Player,
					Defender: gs.GetPlayerSnap(),
				})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		// 1. Ensure prompt is reprinted after handling the message
		defer fmt.Print("> ")

		// 2. Call HandleWar on gamestate with the message body
		outcome, _, _ := gs.HandleWar(rw)

		// 3. Handle outcomes
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			// Requeue so another connected client can consume and test it
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon,
			gamelogic.WarOutcomeYouWon,
			gamelogic.WarOutcomeDraw:
			return pubsub.Ack

		default:
			// Print error and discard for unrecognized outcomes
			fmt.Println("Error: unrecognized war outcome")
			return pubsub.NackDiscard
		}
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
		handlerMove(gameState, publishCh),
	); err != nil {
		log.Fatalf("could not subscribe to army moves queue: %v", err)
	}

	warExchange := routing.ExchangePerilTopic
	warQueueName := fmt.Sprintf("%s", routing.WarRecognitionsPrefix)
	warBindingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	if err := pubsub.SubscribeJSON(
		conn,
		warExchange,
		warQueueName,
		warBindingKey,
		pubsub.SimpleQueueType(0),
		handlerWar(gameState),
	); err != nil {
		log.Fatalf("could not subscribe to war queue: %v", err)
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
