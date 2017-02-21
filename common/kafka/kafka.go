// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"
	"log"
	"os"
	"github.com/golang/glog"
	"github.com/Shopify/sarama"
)

const (
	/*
	## CompressionCodec represents the various compression codecs recognized by
	## Kafka in messages.
	##  0 : No compression
	##  1 : Gzip compression
	##  2 : Snappy compression
	*/
	compression              = 0
	/*
	 ##  RequiredAcks is used in Produce Requests to tell the broker how many
         ##  replica acknowledgements it must see before responding
	 ##   0 : the producer never waits for an acknowledgement from the broker.
	 ##       This option provides the lowest latency but the weakest durability
	 ##       guarantees (some data will be lost when a server fails).
	 ##   1 : the producer gets an acknowledgement after the leader replica has
	 ##       received the data. This option provides better durability as the
	 ##       client waits until the server acknowledges the request as successful
         ##       (only messages that were written to the now-dead leader but not yet
	 ##       replicated will be lost).
	 ##   -1: the producer gets an acknowledgement after all in-sync replicas have
	 ##       received the data. This option provides the best durability, we
         ##       guarantee that no messages will be lost as long as at least one in
	 ##       sync replica remains.
 	 */
	requiredAcks             = 1
	maxRetries               = 3
	metricsTopic             = "heapster-metrics"
	eventsTopic              = "heapster-events"
)

const (
	TimeSeriesTopic = "timeseriestopic"
	EventsTopic     = "eventstopic"
)

type KafkaClient interface {
	Name() string
	Stop()
	ProduceKafkaMessage(msgData interface{}) error
}

type kafkaSink struct {
	producer  sarama.SyncProducer
	dataTopic string
}

func (sink *kafkaSink) ProduceKafkaMessage(msgData interface{}) error {
	start := time.Now()
	msgJson, err := json.Marshal(msgData)
	if err != nil {
		return fmt.Errorf("failed to transform the items to json : %s", err)
	}

	m := &sarama.ProducerMessage{
		Topic: sink.dataTopic,
		Value: sarama.ByteEncoder(string(msgJson)),
	}
	_, _, err = sink.producer.SendMessage(m)
	if err != nil {
		return fmt.Errorf("failed to produce message to %s: %s", sink.dataTopic, err)
	}
	end := time.Now()
	glog.V(4).Infof("Exported %d data to kafka in %s", len([]byte(string(msgJson))), end.Sub(start))
	return nil
}

func (sink *kafkaSink) Name() string {
	return "Apache Kafka Sink"
}

func (sink *kafkaSink) Stop() {
	// nothing needs to be done.
}

func getTopic(opts map[string][]string, topicType string) (string, error) {
	var topic string
	switch topicType {
	case TimeSeriesTopic:
		topic = metricsTopic
	case EventsTopic:
		topic = eventsTopic
	default:
		return "", fmt.Errorf("Topic type '%s' is illegal.", topicType)
	}

	if len(opts[topicType]) > 0 {
		topic = opts[topicType][0]
	}

	return topic, nil
}

func NewKafkaClient(uri *url.URL, topicType string) (KafkaClient, error) {
	opts, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parser url's query string: %s", err)
	}
	glog.V(3).Infof("kafka sink option: %v", opts)

	topic, err := getTopic(opts, topicType)
	if err != nil {
		return nil, err
	}

	var kafkaBrokers []string
	if len(opts["brokers"]) < 1 {
		return nil, fmt.Errorf("There is no broker assigned for connecting kafka")
	}
	kafkaBrokers = append(kafkaBrokers, opts["brokers"]...)
	glog.V(4).Infof("initializing kafka sink with brokers - %v", kafkaBrokers)

	config := sarama.NewConfig()
	config.ClientID = topic
	config.Producer.Compression = sarama.CompressionCodec(compression)
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.RequiredAcks(requiredAcks)
	config.Producer.Retry.Max = maxRetries

	producer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		return nil, err
	}

	// Kafka log redirect to stderr
	sarama.Logger = log.New(os.Stdout, "[Sarama]", log.LstdFlags)


	return &kafkaSink{
		producer:  producer,
		dataTopic: topic,
	}, nil
}
