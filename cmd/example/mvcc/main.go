package main

import (
	"fmt"
	"log"

	mv "github.com/letterbeezps/miniKV/mvcc"
)

func main() {
	db := mv.NewMVCC()
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	err = tx.Set("a", []byte("abdc"))
	if err != nil {
		log.Fatal(err)
	}
	ret, err := tx.Get("a")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ret)
}
