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
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
	kafka_common "k8s.io/heapster/common/kafka"
	"k8s.io/heapster/metrics/core"
	"os"
	"strings"
	"fmt"
	"strconv"
)

type KafkaSinkPoint struct {
	MetricsName      string
	MetricsValue     interface{}
	MetricsTimestamp time.Time
	MetricsTags      map[string]string
}

type kafkaSink struct {
	staticTags map[string]string
	kafka_common.KafkaClient
	sync.RWMutex
	// JSON or Influx
	format    string
	maxRecordSize    int
}

func (sink *kafkaSink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()

	if sink.format == "json" {
		sink.exportJsonData(dataBatch)
	} else {
		sink.exportInfluxData(dataBatch)
	}
}


func NewKafkaSink(uri *url.URL) (core.DataSink, error) {
	client, err := kafka_common.NewKafkaClient(uri, kafka_common.TimeSeriesTopic)
	if err != nil {
		return nil, err
	}

	opts, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parser url's query string: %s", err)
	}

	format := "json"
	if len(opts["format"]) > 0 {
		if format == "json" || format == "influx" {
			format = opts["format"][0]
		}
	}

	maxRecordSize := 1000
	if len(opts["batchsize"]) > 0 {
		maxRecordSize, err = strconv.Atoi(opts["batchsize"][0])
		if err != nil {
			return nil, fmt.Errorf("batchsize should be a integer", err)
		}

		if maxRecordSize <= 0 {
			return nil, fmt.Errorf("batchsize should be above 0")
		}
	}

	tags := make(map[string]string)
	tagStr := os.Getenv("TAGS")
	if tagStr != "" {
		tagSplits := strings.Split(tagStr, ";")
		for _, split := range tagSplits {
			kv := strings.Split(split, "=")
			if len(kv) == 2 {
				tags[kv[0]] = kv[1]
			}
		}
	}


	glog.Info("Global tags: ", tags)

	return &kafkaSink{
		KafkaClient: client,
		staticTags:  tags,
		format: format,
		maxRecordSize: maxRecordSize,
	}, nil
}
