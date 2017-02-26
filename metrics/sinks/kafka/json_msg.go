package kafka

import (
	"k8s.io/heapster/metrics/core"
	"github.com/golang/glog"
)

var pointsBuf = make ([]KafkaSinkPoint, 0, 0)

func (sink *kafkaSink) exportJsonData(dataBatch *core.DataBatch) {

	for _, metricSet := range dataBatch.MetricSets {
		for metricName, metricValue := range metricSet.MetricValues {
			labels := make(map[string]string)
			for k, v := range metricSet.Labels {
				labels[k] = v
			}

			for k, v := range sink.staticTags {
				labels[k] = v
			}

			point := KafkaSinkPoint{
				MetricsName: metricName,
				MetricsTags: labels,
				MetricsValue: map[string]interface{}{
					"value": metricValue.GetValue(),
				},
				MetricsTimestamp: dataBatch.Timestamp.UTC(),
			}
			sink.pushJsonPoint(point)
		}
		for _, metric := range metricSet.LabeledMetrics {
			labels := make(map[string]string)
			for k, v := range metricSet.Labels {
				labels[k] = v
			}
			for k, v := range metric.Labels {
				labels[k] = v
			}

			for k, v := range sink.staticTags {
				labels[k] = v
			}

			point := KafkaSinkPoint{
				MetricsName: metric.Name,
				MetricsTags: labels,
				MetricsValue: map[string]interface{}{
					"value": metric.GetValue(),
				},
				MetricsTimestamp: dataBatch.Timestamp.UTC(),
			}
			sink.pushJsonPoint(point)
		}
	}

	sink.flushJsonPoints()
}

func (sink *kafkaSink) flushJsonPoints() {
	if len(pointsBuf) > 0 {
		sink.sendJsonMessage()
		pointsBuf = make([]KafkaSinkPoint, 0, 0)
	}
}

func (sink *kafkaSink) pushJsonPoint(p KafkaSinkPoint) {
	pointsBuf = append(pointsBuf, p)
	if len(pointsBuf) >= sink.maxRecordSize {
		sink.sendJsonMessage()
		pointsBuf = make([]KafkaSinkPoint, 0, 0)
	}
}

func (sink *kafkaSink) sendJsonMessage() {
	err := sink.ProduceKafkaMessage(pointsBuf)
	if err != nil {
		glog.V(1).Info("Failed to produce metric message which contains %d: %s", len(pointsBuf), err)
	} else {
		glog.V(4).Infof("%d records were sent", len(pointsBuf))
	}
}