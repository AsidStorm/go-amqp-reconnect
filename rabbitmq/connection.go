package rabbitmq

import (
	"crypto/tls"
	"github.com/streadway/amqp"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var Debug bool
var ReconnectDelay = time.Second * 3

type Connection struct {
	*amqp.Connection
	closed int32
	mutex  *sync.Mutex
}

func DialConfig(url string, config amqp.Config) (*Connection, error) {
	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		Connection: conn,
		mutex:      &sync.Mutex{},
	}

	go func() {
		for {
			reason, ok := <-c.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				debugf("connection closed")
				break
			}

			debugf("connection closed, reason: %s", reason)

			for {
				// wait 1s for reconnect
				time.Sleep(ReconnectDelay)

				conn, err := amqp.Dial(url)
				if err != nil {
					debugf("reconnect failed, err: %v", err)
					continue
				}

				c.mutex.Lock()
				c.Connection = conn
				c.mutex.Unlock()

				debugf("reconnect success")
				break
			}
		}
	}()

	return c, nil
}

func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		Connection: conn,
		mutex:      &sync.Mutex{},
	}

	go func() {
		for {
			reason, ok := <-c.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				debugf("connection closed")
				break
			}

			debugf("connection closed, reason: %s", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(ReconnectDelay)

				conn, err := amqp.Dial(url)
				if err != nil {
					debugf("reconnect failed, err: %v", err)
					continue
				}

				c.mutex.Lock()
				c.Connection = conn
				c.mutex.Unlock()

				debugf("reconnect success")
				break
			}
		}
	}()

	return c, nil
}

func (c *Connection) Channel() (*Channel, error) {
	defer c.mutex.Unlock()
	c.mutex.Lock()

	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
		mutex:   &sync.Mutex{},
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.isClosed() {
				debugf("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			debugf("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(ReconnectDelay) // Retry interval: Exponential backoff #2

				if c.Connection.IsClosed() {
					debugf("channel recreate failed, connection still closed")
					continue
				}

				ch, err := c.Connection.Channel()
				if err != nil {
					debugf("channel recreate failed, err: %s", err)
					continue
				}

				// We need to apply Qos settings assigned with this channel
				if channel.qos.applied {
					err := ch.Qos(channel.qos.prefetchCount, channel.qos.prefetchSize, channel.qos.global)

					if err != nil {
						ch.Close()
						debugf("channel recreate failed, unable to restore qos settings, err: %s (prefetch_count: %d; prefetch_size: %d; global: %t)", err, channel.qos.prefetchCount, channel.qos.prefetchSize, channel.qos.global)
						continue
					}

					debugf("qos restored (prefetch_count: %d; prefetch_size: %d; global: %t)", channel.qos.prefetchCount, channel.qos.prefetchSize, channel.qos.global)
				}

				// We need to restore auto deleted queues created with this channel

				queuesRestored := true

				for _, q := range channel.autoDeletedQueues {
					if q.passive {
						_, err = ch.QueueDeclarePassive(q.name, q.durable, q.autoDelete, q.exclusive, q.noWait, q.args)
					} else {
						_, err = ch.QueueDeclare(q.name, q.durable, q.autoDelete, q.exclusive, q.noWait, q.args)
					}

					if err != nil {
						debugf("channel recreate failed, unable to restore auto deleted queue, err: %v (name: %s; durable: %t; auto_delete: %t; exclusive: %t; no_wait: %t; passive: %t)", err, q.name, q.durable, q.autoDelete, q.exclusive, q.noWait, q.passive)
						queuesRestored = false
						break
					}

					debugf("queue restored (name: %s; durable: %t; auto_delete: %t; exclusive: %t; no_wait: %t; passive: %t)", q.name, q.durable, q.autoDelete, q.exclusive, q.noWait, q.passive)
				}

				if !queuesRestored {
					ch.Close()
					continue
				}

				exchangesRestored := true

				for _, e := range channel.autoDeletedExchanges {
					if e.passive {
						err = ch.ExchangeDeclarePassive(e.name, e.kind, e.durable, e.autoDelete, e.internal, e.noWait, e.args)
					} else {
						err = ch.ExchangeDeclare(e.name, e.kind, e.durable, e.autoDelete, e.internal, e.noWait, e.args)
					}

					if err != nil {
						debugf("channel recreate failed, unable to restore auto deleted exchange, err: %v (name: %s; kind: %s; durable: %t; auto_delete: %t; internal: %t; no_wait: %t)", err, e.name, e.kind, e.durable, e.autoDelete, e.internal, e.noWait)
						exchangesRestored = false
						break
					}

					debugf("exchange restored (name: %s; kind: %s; durable: %t; auto_delete: %t; internal: %t; no_wait: %t)", e.name, e.kind, e.durable, e.autoDelete, e.internal, e.noWait)
				}

				if !exchangesRestored {
					ch.Close()
					continue
				}

				channel.mutex.Lock()
				channel.Channel = ch // Concurrency?
				channel.mutex.Unlock()

				debugf("channel recreate success")

				break
			}
		}

	}()

	return channel, nil
}

func (c *Connection) Close() error {
	defer c.mutex.Unlock()
	c.mutex.Lock()

	if c.isClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&c.closed, 1)

	return c.Connection.Close()
}

func (c *Connection) LocalAddr() net.Addr {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.Connection.LocalAddr()
}

func (c *Connection) ConnectionState() tls.ConnectionState {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.Connection.ConnectionState()
}

func (c *Connection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.Connection.NotifyClose(receiver)
}

func (c *Connection) NotifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.Connection.NotifyBlocked(receiver)
}

func (c *Connection) IsClosed() bool {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.Connection.IsClosed()
}

func (c *Connection) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}
