// subscriber.go provides an interface for creating a Subscriber which abstracts
// the process of creating a channel, declaring a queue, and binding the queue
// to an exchange. It provides a function for subscribing to data.

package amqp

import (
	"github.com/streadway/amqp"
)

// Subscriber type
type Subscriber struct {
	channel    *amqp.Channel
	connection *Connection
	id         int
	msgs       chan amqp.Delivery
	options    *SubscriberOptions
	queue      amqp.Queue
}

// SubscriberOptions represents the configuration options that are
// available when creating a Subscriber.
type SubscriberOptions struct {

	//
	// Queue Options
	//

	// An optional name for the queue. When empty, the server
	// will generate a unique name
	QueueName string

	// Durable queues will survive a server restart
	// NOTE: The exchange which the queue is bound to must also be durable.
	Durable bool

	// Auto-deleted queues will be removed after a short time when the last
	// consumer is canceled or the last consumer's channel is closed.
	AutoDelete bool

	// Exclusive queues are only accessible by the connection that
	// declares them and will be deleted when the connection closes.
	Exclusive bool

	//
	// Bind Options
	//

	// The routing key for the queue
	RoutingKey string

	// The name of the exchange to bind the queue to
	ExchangeName string

	// Additional arguments when binding a queue
	BindArgs map[string]interface{}

	//
	// Consume Options
	//

	// When autoAck flag is set, the server will automatically acknowledge deliveries
	// to this subscriber prior to writing the message to the network. When unset,
	// the client must call Delivery.Ack to manually acknowledge the delivery.
	AutoAck bool
}

// NewSubscriber creates an instance of a Subscriber
func (conn *Connection) NewSubscriber(opts *SubscriberOptions) (*Subscriber, error) {

	// Create the subscriber
	sub := &Subscriber{
		connection: conn,
		msgs:       make(chan amqp.Delivery),
		options:    opts,
	}

	// Setup the subscriber
	err := sub.setup(conn, opts)
	if err != nil {
		return nil, err
	}

	// Add the subscriber to the list of participants
	sub.id = conn.addParticipant(sub)

	return sub, nil

}

//
// Subscribe return messages that have been delivered to this subscriber.
//
// This channel does not close even when the connection to the broken
// is lost. The user must call Destroy() on the subscriber to close this
// channel.
//
func (sub *Subscriber) Subscribe() <-chan amqp.Delivery {
	return sub.msgs
}

// Destroy closes the AMQP channel for the subscriber
func (sub *Subscriber) Destroy() error {
	close(sub.msgs)
	sub.connection.removeParticipant(sub.id)
	return sub.channel.Close()
}

// Setup configures the subscriber
func (sub *Subscriber) setup(conn *Connection, opts *SubscriberOptions) (err error) {

	// Open a channel through the connection
	sub.channel, err = conn.Channel()
	if err != nil {
		return
	}

	// Create a new queue
	sub.queue, err = sub.channel.QueueDeclare(
		opts.QueueName,
		opts.Durable,
		opts.AutoDelete,
		opts.Exclusive,
		false,
		nil,
	)
	if err != nil {
		return
	}

	// Bind the queue to the exchange
	err = sub.channel.QueueBind(
		sub.queue.Name,
		opts.RoutingKey,
		opts.ExchangeName,
		false,
		opts.BindArgs,
	)
	if err != nil {
		return
	}

	// Consume messages from the queue
	msgs, err := sub.channel.Consume(
		sub.queue.Name, // queue
		"",             // consumer
		opts.AutoAck,   // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return
	}

	// Start a goroutine to forward data from the amqp provided channel
	// to the subscriber's msg channel.
	go func() {
		for {
			msg, ok := <-msgs
			if !ok {
				break
			}
			sub.msgs <- msg
		}
	}()

	return

}

// Reinit reinitializes the Subscriber with the provided connection
func (sub *Subscriber) reinit() (err error) {
	return sub.setup(sub.connection, sub.options)
}
