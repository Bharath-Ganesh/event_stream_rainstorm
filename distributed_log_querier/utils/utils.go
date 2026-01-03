package utils

import (
	"fmt"
	"os"
)

func CheckError(err error, printMessage string) {
	if err != nil {
		fmt.Println(printMessage, err)
		os.Exit(1)
	}
}
