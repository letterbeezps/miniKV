# miniKV

A simple key-value storage engine in GO. The main purpose of this project is to learn database basics. including but not limited to:

* transaction with MVCC-based snapshot isloation
* Write-Ahead Log (WAL)
* LSM-tree
* ... ...

More pratices are still begin planned.

## Architecture

![architecture](./note/img/miniKV/minikv.png)

## example

### GetSet

```go
package main

import (
    "fmt"
    "log"

    mv "github.com/letterbeezps/miniKV/mvcc"
)

func main() {
    db := mv.NewMVCC()
    tx1, err := db.Begin(false)
    if err != nil {
        log.Fatal(err)
    }
    err = tx1.Set("a", []byte("abdc"))
    if err != nil {
        log.Fatal(err)
    }
    ret, err := tx1.Get("a")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(ret)
    if err := tx1.Commit(); err != nil {
        log.Fatal(err)
    }

    tx2, err := db.Begin(false)
    if err != nil {
        log.Fatal(err)
    }
    for _, c := range []string{"b", "c"} {
        err := tx2.Set(c, []byte(c+"_2"))
        if err != nil {
            log.Fatal(err)
        }
    }
    if err := tx2.Commit(); err != nil {
        log.Fatal(err)
    }
    tx3, err := db.Begin(true)
    if err != nil {
        log.Fatal(err)
    }
    iter, err := tx3.Iter("a", "c")
    if err != nil {
        log.Fatal(err)
    }
    for iter.IsValid() {
        fmt.Println(iter.Key())
        fmt.Println(string(iter.Value()))
        if err := iter.Next(); err != nil {
            log.Fatal(err)
        }
    }
}
```
