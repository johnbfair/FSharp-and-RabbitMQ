FSharp-Agents-and-Queues
========================

A POC of F# Agents along with a few different techniques of message queue management.

##Preliminary Results:
* CBR (Content-based routing): ~2800 (durable) msgs/sec @100k messages
* Rabbit Routing: ~8000 (durable) msgs/sec @100k mesages

##Setup:
1. Install [RabbitMQ](http://www.rabbitmq.com/)
2. Create 5 queues: *cbr-main*, *cbr-message1*, *cbr-message2*, *routing-message1*, *routing-message2*
 * These are meant to be created manually in RabbitMQ via the [Management plugin](http://www.rabbitmq.com/management.html).  
   Administration of RabbitMQ queues and exchanges is outside of the scope of this POC.
3. Add 2 new bindings to this exchange: *amq.direct*
 * To Queue: *routing-message1*
 * Routing key: *message1*
 * To Queue: *routing-message2*
 * Routing key: *message2*
