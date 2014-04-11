papackage com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.MetricDisruptor;

public class MetricsConsumer extends Consumer {

    @Inject
    public MetricsConsumer(KafkaMetricsConsumer kafkaConsumer, MetricDisruptor disruptor) {
        super(kafkaConsumer, disruptor);
    }
}
