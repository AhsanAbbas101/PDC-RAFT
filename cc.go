package main

import (
	"bufio"
	"fmt"
	"os"
)

var max_options int = 2

// Displays option menu
func DisplayMenu() {
	fmt.Println()
	fmt.Println("Please select an option:")
	fmt.Println("1 – UP : turn on a node.")
	fmt.Println("2 – DOWN : turn off a node. ")
	fmt.Println("0 – EXIT : exit command center. ")

	fmt.Println()

}

func main() {

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
			} else if input < 0 || input > max_options  {
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
			
		case 2:
			fmt.Printf("Covid19 cases in SouthKorea: %d\n", cases[1])
		case 3:
			fmt.Printf("Covid19 cases in France: %d\n", cases[2])
		case 4:
			fmt.Printf("Enter Message >> ")
			in := bufio.NewReader(os.Stdin)
			line, _ := in.ReadString('\n') // Read input with spaces
			fmt.Printf("Your message: %s\n", line)
		}
	}

}
