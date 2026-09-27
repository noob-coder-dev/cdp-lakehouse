package com.cdp.lakehouse.cdc;

import com.cdp.lakehouse.cdc.CdcNormalizeFunction.TableContract;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Raw CDC topics to clean topics, exactly once (spec 01, section 4.3).
 * Submitted to the Flink session cluster on EKS; checkpoints every 30–60 s to S3.
 */
public final class CdcNormalizerJob {

    private CdcNormalizerJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.getRequired("bootstrap-servers");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(params.getLong("checkpoint-interval-ms", 60_000L));   // exactly-once mode is the default

        KafkaSource<RawRecord> source = KafkaSource.<RawRecord>builder()
                .setBootstrapServers(brokers)
                .setTopicPattern(Pattern.compile(params.get("raw-topic-pattern", "raw\\.cdc\\..+")))
                .setGroupId("cdc-normalizer")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RawRecordDeserializer())
                .build();

        // One transactional sink for clean, DLQ and HOLD records, so a checkpoint's outputs commit together.
        // The transaction timeout must outlast the worst crash-to-recovery time; otherwise Kafka aborts the pending
        // transaction and its records are lost. The "every record accounted for" check (#3) catches that, and the
        // missing range is replayed from raw.
        KafkaSink<OutRecord> sink = KafkaSink.<OutRecord>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(new OutRecordSerializer())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("cdc-normalizer")
                .setProperty("transaction.timeout.ms", params.get("transaction-timeout-ms", "900000"))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "raw-cdc")
                .uid("raw-cdc-source")
                .flatMap(new CdcNormalizeFunction(
                        onboardedTables(),
                        params.getRequired("schema-registry-url"),
                        params.get("dlq-topic", "dlq.cdc"),
                        params.get("hold-topic", "hold.cdc")))
                .uid("cdc-normalize")
                .sinkTo(sink)
                .uid("cdc-sink");

        env.execute("cdc-normalizer");
    }

    /** Onboarded tables by raw topic. Generated from the contract repo; stubbed here with one example. */
    static Map<String, TableContract> onboardedTables() {
        return Map.of("raw.cdc.lending.loan_account", new TableContract(
                TableContract.Source.POSTGRES,
                "clean.cdc.lending.loan_account",
                List.of("loan_id"),
                List.of("loan_id", "customer_id", "principal_paise", "outstanding_paise", "status", "remarks",
                        "updated_at")));
    }

    /** Keeps the raw bytes and where they came from; decoding happens in the keyed function. */
    static final class RawRecordDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) {
            out.collect(new RawRecord(record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }

        @Override
        public TypeInformation<RawRecord> getProducedType() {
            return TypeInformation.of(RawRecord.class);
        }
    }

    /** Sends each record to the topic its route chose. Rejects carry the reason and raw position as headers. */
    static final class OutRecordSerializer implements KafkaRecordSerializationSchema<OutRecord> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(OutRecord r, KafkaSinkContext context, Long timestamp) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(r.topic, r.key, r.value);
            if (r.rejectReason != null) {
                record.headers().add("cdc.reject_reason", r.rejectReason.getBytes(StandardCharsets.UTF_8));
                record.headers().add("cdc.raw_position", r.rawPosition.getBytes(StandardCharsets.UTF_8));
            }
            return record;
        }
    }
}
