/*
	Command Center
*/

package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"net"
)

var max_options int = 4
var leaderPort string

// Displays option menu
func DisplayMenu() {
	fmt.Println()
	fmt.Println("Please select an option:")
	fmt.Println("1 - SET : set value of data ")
	fmt.Println("2 - ADD : add value to data ")
	fmt.Println("3 - BLACKOUT : simulate network partition ")
	fmt.Println("4 - LIGHTSON : heal network partition ")
	fmt.Println("0 – EXIT : exit command center. ")

	fmt.Println()

}

type Peer struct {
	Port string
}
type Message struct {
	MessageType MessageTypeEnum
	Data        interface{}
}
type Command struct {
	Operation OperationEnum
	Data      int
}

func sendMessage(msg Message, port string) {

	conn, err := net.Dial("tcp", "localhost:"+port)
	if err != nil {
		fmt.Println("[E] Error Dailing Port ", port)
		return
	}

	gobEncoder := gob.NewEncoder(conn)
	err = gobEncoder.Encode(msg)
	if err != nil {
		log.Println(err)
	}

	defer conn.Close()
}

func main() {

	gob.Register(Command{})
	gob.Register(Message{})
	gob.Register(Peer{})

	// Get Leader Port
	flag.Parse()
	PortsArray := flag.Args()
	if len(PortsArray) >= 1 {
		leaderPort = PortsArray[0]
	} else {
		leaderPort = "2020"
	}

	fmt.Println(PortsArray)

	execute := true
	for execute == true {

		DisplayMenu()

		var input int
		validate_input := true

		// Validate Input
		for validate_input == true {

			fmt.Printf("Input >> ")
			_, err := fmt.Scan(&input)

			if err != nil {
				fmt.Println("Error: ", err)
			} else if input < 0 || input > max_options {
				fmt.Println("Invalid Input! Please Enter Again.")
			} else {
				validate_input = false
			}

			var discard string
			fmt.Scanln(&discard) // Flush

		}

		switch input {

		case 0:
			fmt.Println("Exiting...")
			execute = false
		case 1:
			fmt.Println("Node SET")
			fmt.Printf("Input >> ")
			_, err := fmt.Scan(&input)
			if err != nil {
				fmt.Println("Error: ", err)
			}
			go sendMessage(Message{LogCommand, Command{SET, input}}, leaderPort)
		case 2:
			fmt.Println("Node ADD")
			fmt.Printf("Input >> ")
			_, err := fmt.Scan(&input)
			if err != nil {
				fmt.Println("Error: ", err)
			}
			go sendMessage(Message{LogCommand, Command{ADD, input}}, leaderPort)

		case 3:
			fmt.Println("Going Dark...")
			go sendMessage(Message{Blackout, Peer{leaderPort}}, leaderPort)
		case 4:
			fmt.Println("Let there be light...")
			go sendMessage(Message{LightsOn, Peer{leaderPort}}, leaderPort)
		}
	}

}
