# go-amqp-reconnect

Auto reconnecting library for https://github.com/rabbitmq/amqp091-go, that doesn't reconnect on server shutdown
To simply developers, here is auto reconnect wrap with detail comments.

This library based on https://github.com/isayme/go-amqp-reconnect, but also supports some external methods.

## Methods

1. `rabbitmq.Dial(url)` & `rabbitmq.DialConfig(url, config)` - creates connection with reconnect
2. `connection.Channel()` - creates channel with reconnect
3. `connection.Close()` - clearly closes channel without reconnect
4. `channel.Qos()` - restore settings for channel
5. `channel.QueueDeclare()` & `channel.QueueDeclarePassive()` - restore autoDeleted queues
6. `channel.ExchangeDeclare()` & `channel.ExchangeDeclarePassive()` - restores autoDeleted exchanges
7. `channel.Close()` - clearly closes channel without reconnect

Another methods call through aliases with mutex, to avoid race condition

## How to change existing code
1. Add import `import "github.com/AsidStorm/go-amqp-reconnect/rabbitmq"`
2. Replace `amqp.Connection`/`amqp.Channel` with `rabbitmq.Connection`/`rabbitmq.Channel`!

## Debug

You can set `rabbitmq.Debug = true` variable to view debug messages

## Settings

1. `rabbitmq.ReconnectDelay` determines time that app wait for new reconnect try, defaults to `time.Second * 3`

## Examples

### Close by developer

```bash
go run examples/close_by_developer/demo.go -url=amqp://user:password@host:port/
```

### Close with reconnect

```bash
go run examples/close_with_reconnect/demo.go -url=amqp://user:password@host:port/
```

After start, drop connection through RabbitMQ management panel on connection tabs, or restart RabbitMQ server

### Graceful Shutdown

```bash
go run examples/graceful_shutdown/demo.go -url=amqp://user:password@host:port/
```

After start try to interrupt with (Ctrl or Cmd + C) app, see that all messages processed and only after that all connections closed