package kafka

import (
	"k8s.io/heapster/metrics/core"
	"github.com/golang/glog"
	influxdb "github.com/influxdata/influxdb/client"
	"strings"
)

var lineBuf = make([]string, 0, 0)

func (sink *kafkaSink) exportInfluxData(dataBatch *core.DataBatch) {
	for _, metricSet := range dataBatch.MetricSets {
		for metricName, metricValue := range metricSet.MetricValues {
			var value interface{}
			if core.ValueInt64 == metricValue.ValueType {
				value = metricValue.IntValue
			} else if core.ValueFloat == metricValue.ValueType {
				value = float64(metricValue.FloatValue)
			} else {
				continue
			}

			fieldName := "value"
			tags := make(map[string]string)
			for k, v := range metricSet.Labels {
				tags[k] = v
			}

			for k, v := range sink.staticTags {
				tags[k] = v
			}
			measurementName := metricName
			point := influxdb.Point{
				Measurement: measurementName,
				Tags:        tags,
				Fields: map[string]interface{}{
					fieldName: value,
				},
				Time: dataBatch.Timestamp.UTC(),
			}


			sink.pushPoint(point)
		}
		for _, labeledMetric := range metricSet.LabeledMetrics {
			var value interface{}
			if core.ValueInt64 == labeledMetric.ValueType {
				value = labeledMetric.IntValue
			} else if core.ValueFloat == labeledMetric.ValueType {
				value = float64(labeledMetric.FloatValue)
			} else {
				continue
			}

			// Prepare measurement without fields
			fieldName := "value"
			measurementName := labeledMetric.Name

			point := influxdb.Point{
				Measurement: measurementName,
				Tags:        make(map[string]string),
				Fields: map[string]interface{}{
					fieldName: value,
				},
				Time: dataBatch.Timestamp.UTC(),
			}
			for key, value := range metricSet.Labels {
				point.Tags[key] = value
			}
			for key, value := range labeledMetric.Labels {
				point.Tags[key] = value
			}

			for key, value := range sink.staticTags {
				point.Tags[key] = value
			}

			sink.pushPoint(point)
		}
	}

	sink.flush()
}

func (sink *kafkaSink) flush() {
	if len(lineBuf) > 0 {
		sink.sendStringMessage()
		lineBuf = make([]string, 0, 0)
	}
}

func (sink *kafkaSink) pushPoint(p influxdb.Point) {
	lineBuf = append(lineBuf, p.MarshalString())
	if len(lineBuf) >= sink.maxRecordSize {
		sink.sendStringMessage()
		lineBuf = make([]string, 0, 0)
	}
}

func (sink *kafkaSink) sendStringMessage() {
	msg := strings.Join(lineBuf, "\n")
	err := sink.ProduceKafkaStringMessage(msg)
	if err != nil {
		glog.V(1).Info("Failed to produce metric message which contains %d: %s", len(lineBuf), err)
	} else {
		glog.V(4).Infof("%d records were sent", len(lineBuf))
	}
}