package org.mason77.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.mason77.constant.StreamConstant;

public class FlinkSinkUtil {
    /**
     * 构建kafka的sink，也许可以加一个参数来控制是否开启精确一致性
     * @param topic
     * @return
     */
    public static KafkaSink<String> getKafkaSink(String topic) {
       return KafkaSink.<String>builder()
               .setBootstrapServers(StreamConstant.KAFKA_BROKERS)
               .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                       .setTopic(topic)
                       .setValueSerializationSchema(new SimpleStringSchema())
                       .build())
               // 开启事务，精确一致性
//               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//               .setTransactionalIdPrefix("dwd_base_log_")
//               .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
               .build();
    }
}
