rabbit
======

A RabbitMQ wrapper lib around [streadway/amqp](https://github.com/streadway/amqp) 
with some bells and whistles.

* Support for auto-reconnect
* Support for context (ie. cancel/timeout)
* ???

# Motivation

We (Batch), make heavy use of RabbitMQ - we use it as the primary method for
facilitating inter-service communication. Due to this, all services make use of
RabbitMQ and are both publishers and consumers.

We wrote this lib to ensure that all of our services make use of Rabbit in a
consistent, predictable way AND are able to survive network blips.

# Usage
```go
package main

import (
    "fmt"
    "log"  

    "github.com/batchcorp/rabbit"
)

func main() { 
    r, err := rabbit.New(&rabbit.Options{
        // instantiate
    })
    if err != nil {
        log.Fatalf("unable to instantiate rabbit: %s", err)
    }
    
    routingKey := "messages"
    data := []byte("pumpkins")

    // Publish something
    if err := r.Publish(ctx, routingKey, data); err != nil {
        log.Fatalf("unable to publish message: ")
    }

    // Consume once
    if err := r.ConsumeOnce(ctx, func() amqp.Delivery) {
        fmt.Printf("Received new message: %+v\n", msg)
    }); err != nil {
        log.Fatalf("unable to consume once: %s", err),
    }

    var numReceived int

    // Consume forever (blocks)
    if err := r.Consume(ctx, func(msg amqp.Delivery) {
        fmt.Printf("Received new message: %+v\n", msg)
        
        numReceived++
        
        if numReceived > 1 {
            r.Stop()
        }
    }); err != nil {
        log.Fatalf("ran into error during consume: %s", err)
    }
    
    fmt.Println("Finished")
}
```
