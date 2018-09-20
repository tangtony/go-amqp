// publisher.go provides an interface for creating a Publisher which abstracts
// the process of creating a channel and declaring an exchange. It provides
// a function for publishing data based on the AMQP specification.

package amqp

import (
	"time"

	"github.com/streadway/amqp"
)

// ExchangeKind type
type ExchangeKind string

// ExchangeKind options
const (

	// A direct exchange delivers messages to queues based on the message routing key.
	Direct ExchangeKind = "direct"

	// A fanout exchange routes messages to all of the queues
	// that are bound to it and the routing key is ignored.
	Fanout ExchangeKind = "fanout"

	// A topic exchange routes messages to one or many queues based on matching between
	// a message routing key and the pattern that was used to bind a queue to an exchange.
	Topic ExchangeKind = "topic"

	// A headers exchange is designed for routing on multiple attributes that are more
	// easily expressed as message headers than a routing key. Headers exchanges
	// ignore the routing key attribute. Instead, the attributes used for routing are
	// taken from the headers attribute. A message is considered matching if the value
	// of the header equals the value specified upon binding.
	Headers ExchangeKind = "headers"
)

// Publisher type
type Publisher struct {
	channel    *amqp.Channel
	connection *Connection
	id         int
	options    *PublisherOptions
}

// PublisherOptions represents the configuration options that are
// available when creating a Publisher.
type PublisherOptions struct {

	// The name of the exchange
	ExchangeName string

	// The type of exchange
	ExchangeKind ExchangeKind

	// Durable exchanges will survive a server restart
	Durable bool

	// Auto-deleted exchanges will be removed when there are no remaining bindings
	AutoDelete bool
}

// Message defines the client message sent to the server. This type mirrors the
// definition of amqp.Publishing and is re-defined here for convenience.
type Message struct {

	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers map[string]interface{}

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationID   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageID       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserID          string    // creating user id - ex: "guest"
	AppID           string    // creating application id

	// The application specific payload of the message
	Body []byte
}

// NewPublisher creates an instance of a Publisher
func (conn *Connection) NewPublisher(opts *PublisherOptions) (*Publisher, error) {

	// Create the publisher
	pub := &Publisher{
		connection: conn,
		options:    opts,
	}

	// Setup the publisher
	err := pub.setup(conn, opts)
	if err != nil {
		return nil, err
	}

	// Add the publisher to the list of participants
	pub.id = conn.addParticipant(pub)

	// Return the publisher
	return pub, nil

}

//
// Publish asynchronously sends a message from the client to an exchange
// on the server.
//
// The routing key is used for routing messages depending on the exchange
// configuration.
//
// The mandatory flag tells the server how to react if the message cannot
// be routed to a queue. When set, the server will return an unroutable
// message with a Return method. When unset, the server silently drops the
// message.
//
// The immediate flag tells the server how to react if the message cannot
// be routed to a queue consumer immediately. When set, the server will
// return an undeliverable message with a Return method. When unset, the
// server will queue the message, but with no guarantee that it will ever be
// consumed.
//
// A listener (Channel.NotifyReturn) can be added handle any undeliverable
// message when calling Publish with either the mandatory or immediate
// flags set.
//
// See the underlying implementation for more information.
//
func (pub *Publisher) Publish(routingKey string, mandatory, immediate bool, msg Message) error {
	return pub.channel.Publish(
		pub.options.ExchangeName,
		routingKey,
		mandatory,
		immediate,
		amqp.Publishing{
			Headers:         msg.Headers,
			ContentType:     msg.ContentType,
			ContentEncoding: msg.ContentEncoding,
			DeliveryMode:    msg.DeliveryMode,
			Priority:        msg.Priority,
			CorrelationId:   msg.CorrelationID,
			ReplyTo:         msg.ReplyTo,
			Expiration:      msg.Expiration,
			MessageId:       msg.MessageID,
			Timestamp:       msg.Timestamp,
			Type:            msg.Type,
			UserId:          msg.UserID,
			AppId:           msg.AppID,
			Body:            msg.Body,
		},
	)
}

// Destroy closes the AMQP channel for the publisher
func (pub *Publisher) Destroy() error {
	pub.connection.removeParticipant(pub.id)
	return pub.channel.Close()
}

// Setup configures the publisher
func (pub *Publisher) setup(conn *Connection, opts *PublisherOptions) (err error) {

	// Open a channel through the connection
	pub.channel, err = conn.Channel()
	if err != nil {
		return
	}

	// Create the exchange
	err = pub.channel.ExchangeDeclare(
		opts.ExchangeName,
		string(opts.ExchangeKind),
		opts.Durable,
		opts.AutoDelete,
		false,
		false,
		nil,
	)

	return

}

// Reinit reinitializes the Publisher with the provided connection
func (pub *Publisher) reinit() (err error) {
	return pub.setup(pub.connection, pub.options)
}
