package kafkabus

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int) error {
	if len(brokers) == 0 {
		return fmt.Errorf("at least one kafka broker is required")
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("kafka topic is required")
	}
	if partitions <= 0 {
		return fmt.Errorf("kafka partitions must be positive")
	}
	if replicationFactor <= 0 {
		return fmt.Errorf("kafka replication factor must be positive")
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("lookup kafka controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial kafka controller %s:%d: %w", controller.Host, controller.Port, err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}); err != nil {
		return fmt.Errorf("create kafka topic %s: %w", topic, err)
	}

	return nil
}
