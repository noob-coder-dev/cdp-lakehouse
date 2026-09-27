package com.cdp.lakehouse.cdc;

import static com.cdp.lakehouse.cdc.CleanRecordCodec.field;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Decides, for every raw Debezium record, whether it can be versioned and applied (spec 01, rules 7 and 9).
 *
 * <p>Raw records were flattened at capture by Debezium's ExtractNewRecordState ({@code __op}, position fields,
 * {@code __source_ts_ms}; deletes arrive as rows) and stamped by Kafka Connect's InsertField with
 * {@code __cdc_epoch} and, for MySQL, {@code __cdc_expected_server}. Each record takes one route:
 * <ul>
 *   <li>CLEAN: the row plus cdc_op, cdc_version, cdc_epoch, src_commit_ts_utc, raw_partition, raw_offset.
 *   <li>DLQ: it can't be versioned (undecodable, unknown op, missing key or position).
 *   <li>HOLD: versioning it would mean guessing (MySQL server changed, contract column missing, unknown table).
 * </ul>
 *
 * <p>Arrival order doesn't matter here: the MERGE in snowflake/01_cdc_apply.sql resolves it by version. So this
 * function keeps no state and runs at any parallelism.
 */
public class CdcNormalizeFunction extends RichFlatMapFunction<RawRecord, OutRecord> {

    /** What the platform knows about one captured table. Generated from the contract repo. */
    public static final class TableContract implements Serializable {
        public enum Source { POSTGRES, MYSQL }

        final Source source;
        final String cleanTopic;
        final List<String> keyColumns;
        final List<String> requiredColumns;

        public TableContract(Source source, String cleanTopic, List<String> keyColumns, List<String> requiredColumns) {
            this.source = Objects.requireNonNull(source);
            this.cleanTopic = Objects.requireNonNull(cleanTopic);
            this.keyColumns = List.copyOf(keyColumns);
            this.requiredColumns = List.copyOf(requiredColumns);
        }
    }

    private static final Set<String> OPS = Set.of("c", "u", "d", "r");

    private final HashMap<String, TableContract> contractsByRawTopic;
    private final String schemaRegistryUrl;
    private final String dlqTopic;
    private final String holdTopic;
    private transient CleanRecordCodec codec;

    public CdcNormalizeFunction(Map<String, TableContract> contractsByRawTopic, String schemaRegistryUrl,
                                String dlqTopic, String holdTopic) {
        this.contractsByRawTopic = new HashMap<>(contractsByRawTopic);
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.dlqTopic = dlqTopic;
        this.holdTopic = holdTopic;
    }

    @Override
    public void open(OpenContext openContext) {
        codec = new CleanRecordCodec(schemaRegistryUrl);
    }

    @Override
    public void flatMap(RawRecord in, Collector<OutRecord> out) {
        if (in.value != null) {              // skip Kafka tombstones: the delete itself arrived as a row
            out.collect(normalize(in));
        }
    }

    private OutRecord normalize(RawRecord in) {
        TableContract contract = contractsByRawTopic.get(in.topic);
        if (contract == null) {
            return reject(holdTopic, in, "no contract for this topic; onboard the table first");
        }
        GenericRecord row;
        try {
            row = codec.decode(in.topic, in.value);
        } catch (RuntimeException e) {
            return reject(dlqTopic, in, "undecodable: " + e.getMessage());
        }
        for (String column : contract.requiredColumns) {
            if (row.getSchema().getField(column) == null) {
                return reject(holdTopic, in, "contract column missing (renamed or dropped upstream?): " + column);
            }
        }
        for (String column : contract.keyColumns) {
            if (field(row, column) == null) {
                return reject(dlqTopic, in, "null key column: " + column);
            }
        }
        String op = text(row, "__op");
        if (op == null || !OPS.contains(op)) {
            return reject(dlqTopic, in, "unknown op: " + op);
        }
        if (contract.source == TableContract.Source.MYSQL) {
            String expected = text(row, "__cdc_expected_server");
            String actual = text(row, "__server_id");
            if (expected == null || !expected.equals(actual)) {
                return reject(holdTopic, in,
                        "binlog from server " + actual + ", expected " + expected + "; bump the epoch before applying");
            }
        }

        long epoch;
        long commitTsMs;
        BigInteger version;
        try {
            epoch = Long.parseLong(text(row, "__cdc_epoch"));
            commitTsMs = number(row, "__source_ts_ms");
            version = "r".equals(op) ? CdcVersion.snapshot(epoch) : switch (contract.source) {
                case POSTGRES -> CdcVersion.postgres(epoch, number(row, "__lsn"));
                case MYSQL -> CdcVersion.mysql(epoch, text(row, "__file"), number(row, "__pos"), number(row, "__row"));
            };
        } catch (IllegalArgumentException e) {
            return reject(dlqTopic, in, "cannot version: " + e.getMessage());
        }
        byte[] clean = codec.encodeClean(contract.cleanTopic, row, op, version, epoch, commitTsMs, in);
        return new OutRecord(contract.cleanTopic, in.key, clean, null, null);
    }

    private OutRecord reject(String topic, RawRecord in, String reason) {
        return new OutRecord(topic, in.key, in.value, reason, in.topic + "/" + in.partition + "/" + in.offset);
    }

    private static String text(GenericRecord row, String name) {
        Object value = field(row, name);
        return value == null ? null : value.toString();
    }

    private static long number(GenericRecord row, String name) {
        if (!(field(row, name) instanceof Number n)) {
            throw new IllegalArgumentException(name + " is missing or not a number");
        }
        return n.longValue();
    }
}
