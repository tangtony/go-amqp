package amqp

import (
	"errors"
	"log"
	"math"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Connection to an AMQP broker
type Connection struct {
	*amqp.Connection
	attempts     int
	mutex        *sync.Mutex
	options      *ConnectionOptions
	participants []Participant
}

// ConnectionOptions are the options for an AMQP connection
type ConnectionOptions struct {
	Username string
	Password string
	Host     string
	Port     string
	Vhost    string
}

// Participant interface for publishers, subscribers, requesters or repliers
type Participant interface {
	Destroy() error
	reinit() error
}

// Maximum timeout when attempting reconnections to the AMQP broker.
var maxReconnectTimeout float64 = 32

// Connect initializes the connection to the AMQP broker
func Connect(opts *ConnectionOptions) (*Connection, error) {

	// Check that a host is specified
	if opts.Host == "" {
		return nil, errors.New("A host must be specified")
	}

	// Create the connection
	conn := &Connection{
		mutex:   &sync.Mutex{},
		options: opts,
	}

	// Connect to AMQP broker
	var err error
	conn.Connection, err = amqp.Dial(getAmqpURI(opts))
	if err != nil {
		return nil, err
	}

	// Start a goroutine that handles close events
	go handleCloseEvents(conn)

	// Return the connection
	return conn, nil

}

// SetMaxReconnectTimeout sets the maximum time to wait between reconnection
// attempts when the connection is lost unexpectedly. This is a global parameter
// that applies to all connections.
func SetMaxReconnectTimeout(t float64) {
	maxReconnectTimeout = t
}

// addParticipant appends the given participant to the connection's list of participants
func (conn *Connection) addParticipant(p Participant) int {
	conn.mutex.Lock()
	conn.participants = append(conn.participants, p)
	id := len(conn.participants)
	conn.mutex.Unlock()
	return id
}

// removeParticipant removes a participant by id from the connection's list of participants
func (conn *Connection) removeParticipant(id int) {
	conn.mutex.Lock()
	copy(conn.participants[id-1:], conn.participants[id:])
	conn.participants[len(conn.participants)-1] = nil
	conn.participants = conn.participants[:len(conn.participants)-1]
	conn.mutex.Unlock()
}

// getAmqpURI converts ConnectionOptions into an AMQP connection string
// See: https://www.rabbitmq.com/uri-spec.html
func getAmqpURI(opt *ConnectionOptions) string {

	amqpUserInfo := ""
	if opt.Username != "" {
		if opt.Password != "" {
			amqpUserInfo = opt.Username + ":" + opt.Password + "@"
		} else {
			amqpUserInfo = opt.Username + "@"
		}
	}

	amqpAuthority := ""
	if opt.Port != "" {
		amqpAuthority = amqpUserInfo + opt.Host + ":" + opt.Port
	} else {
		amqpAuthority = amqpUserInfo + opt.Host
	}

	vhost := ""
	if opt.Vhost != "" {
		vhost = "/" + opt.Vhost
	}

	return "amqp://" + amqpAuthority + vhost

}

// reconnect continually attempts to reconnect to the AMQP broker
func reconnect(conn *Connection) {
	for {

		log.Println("Attempting to reconnect to AMQP broker...")
		var err error
		conn.Connection, err = amqp.Dial(getAmqpURI(conn.options))
		if err == nil {

			log.Println("Connection to AMQP broker restored. Re-initializing participants...")
			for i := 0; i < len(conn.participants); i++ {
				if err := conn.participants[i].reinit(); err != nil {
					log.Println(err)
				}
			}

			log.Println("AMQP participants re-initialized.")
			break

		}

		// Backoff
		delay := math.Min(math.Pow(2, float64(conn.attempts)), maxReconnectTimeout)
		log.Printf("Could not connect to AMQP broker. Waiting for %d seconds...", int(delay))
		time.Sleep(time.Duration(delay) * time.Second)

		// Increment the number of reconnection attempts
		conn.attempts++

	}
}

// handleCloseEvents listens for close events on the connection and attempts to
// reconnect to the broker when the connection is disconnected unexpectedly.
func handleCloseEvents(conn *Connection) {
	for {
		// Reset the number of reconnection attempts
		conn.attempts = 0

		// Wait for the connection to be closed
		if connErr := <-conn.NotifyClose(make(chan *amqp.Error)); connErr == nil {
			log.Println("Connection to AMQP broker closed normally.")
			break
		} else {
			log.Println("Connection to AMQP broker closed unexpectedly. ")
			reconnect(conn)
		}
	}
}
