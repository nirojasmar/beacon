package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type Client struct {
	conn *nats.Conn
	js   nats.JetStreamContext
}

func NewClient(url, user, password string) (*Client, error) {
	conn, err := nats.Connect(url,
		nats.UserInfo(user, password),
		nats.Name("beacon"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}
	return &Client{conn: conn, js: js}, nil
}

func (c *Client) Connected() bool {
	return c != nil && c.conn != nil && c.conn.IsConnected()
}

func (c *Client) Reconnects() uint64 {
	if c == nil || c.conn == nil {
		return 0
	}
	return c.conn.Stats().Reconnects
}

func (c *Client) Close() {
	if c != nil && c.conn != nil {
		if err := c.conn.Drain(); err != nil {
			log.Printf("drain NATS connection: %v", err)
		}
		c.conn.Close()
	}
}

func (c *Client) EnsureInfrastructure(ctx context.Context, checkerConcurrency int) error {
	if checkerConcurrency < 1 {
		checkerConcurrency = 20
	}
	streams := []nats.StreamConfig{
		{
			Name: "JOBS", Subjects: []string{"jobs.run"}, Retention: nats.WorkQueuePolicy,
			Storage: nats.FileStorage, MaxAge: 10 * time.Minute, Duplicates: 2 * time.Minute,
		},
		{
			Name: "RESULTS", Subjects: []string{"results.*"}, Retention: nats.LimitsPolicy,
			Storage: nats.FileStorage, MaxAge: 24 * time.Hour, MaxBytes: 200 * 1024 * 1024,
			Discard: nats.DiscardOld,
		},
		{
			Name: "EVENTS", Subjects: []string{"events.state_change"}, Retention: nats.LimitsPolicy,
			Storage: nats.FileStorage, MaxAge: 7 * 24 * time.Hour,
		},
		{
			Name: "EVENTS_DLQ", Subjects: []string{"events.dlq"}, Retention: nats.LimitsPolicy,
			Storage: nats.FileStorage, MaxAge: 14 * 24 * time.Hour,
		},
	}
	for _, config := range streams {
		if _, err := c.js.StreamInfo(config.Name, nats.Context(ctx)); err == nil {
			continue
		} else if !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("inspect stream %s: %w", config.Name, err)
		}
		if _, err := c.js.AddStream(&config, nats.Context(ctx)); err != nil {
			if _, infoErr := c.js.StreamInfo(config.Name, nats.Context(ctx)); infoErr == nil {
				continue
			}
			return fmt.Errorf("create stream %s: %w", config.Name, err)
		}
	}

	for _, config := range []nats.KeyValueConfig{
		{Bucket: "checks", History: 1},
		{Bucket: "state", History: 5},
	} {
		if _, err := c.js.KeyValue(config.Bucket); err == nil {
			continue
		}
		if _, err := c.js.CreateKeyValue(&config); err != nil {
			if _, openErr := c.js.KeyValue(config.Bucket); openErr == nil {
				continue
			}
			return fmt.Errorf("create KV bucket %s: %w", config.Bucket, err)
		}
	}

	for stream, consumer := range map[string]nats.ConsumerConfig{
		"JOBS":   {Durable: "checkers", AckPolicy: nats.AckExplicitPolicy, AckWait: 35 * time.Second, MaxAckPending: checkerConcurrency},
		"EVENTS": {Durable: "notifier", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Minute, MaxDeliver: 8},
	} {
		if _, err := c.js.ConsumerInfo(stream, consumer.Durable); err == nil {
			continue
		} else if !errors.Is(err, nats.ErrConsumerNotFound) {
			return fmt.Errorf("inspect consumer %s: %w", consumer.Durable, err)
		}
		if _, err := c.js.AddConsumer(stream, &consumer, nats.Context(ctx)); err != nil {
			if _, infoErr := c.js.ConsumerInfo(stream, consumer.Durable); infoErr == nil {
				continue
			}
			return fmt.Errorf("create consumer %s: %w", consumer.Durable, err)
		}
	}
	return nil
}

func (c *Client) KV(bucket string) (nats.KeyValue, error) {
	kv, err := c.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("open KV bucket %s: %w", bucket, err)
	}
	return kv, nil
}

func (c *Client) Publish(ctx context.Context, subject, messageID string, data []byte) error {
	message := nats.NewMsg(subject)
	message.Data = data
	if messageID != "" {
		message.Header.Set(nats.MsgIdHdr, messageID)
	}
	if _, err := c.js.PublishMsg(message, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish to %s: %w", subject, err)
	}
	return nil
}

func (c *Client) Subscribe(subject string) (<-chan *nats.Msg, *nats.Subscription, error) {
	messages := make(chan *nats.Msg, 32)
	subscription, err := c.conn.ChanSubscribe(subject, messages)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe to %s: %w", subject, err)
	}
	return messages, subscription, nil
}

func (c *Client) Ready() error {
	if !c.Connected() {
		return errors.New("NATS is disconnected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.js.AccountInfo(nats.Context(ctx)); err != nil {
		return fmt.Errorf("JetStream unavailable: %w", err)
	}
	return nil
}

func (c *Client) Stream() nats.JetStreamContext {
	return c.js
}
