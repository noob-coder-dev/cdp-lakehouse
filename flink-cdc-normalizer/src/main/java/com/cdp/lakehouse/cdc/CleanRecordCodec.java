package com.cdp.lakehouse.cdc;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** Plumbing: Avro decoding of raw records and encoding of clean ones, through Schema Registry. */
final class CleanRecordCodec {

    private static final Schema VERSION_SCHEMA =
            LogicalTypes.decimal(38, 0).addToSchema(Schema.create(Schema.Type.BYTES));
    private static final Schema TIMESTAMP_SCHEMA =
            LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Conversions.DecimalConversion DECIMAL = new Conversions.DecimalConversion();

    private final KafkaAvroDeserializer deserializer;
    private final KafkaAvroSerializer serializer;
    private final Map<Schema, Schema> cleanSchemas = new HashMap<>();

    CleanRecordCodec(String schemaRegistryUrl) {
        SchemaRegistryClient registry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1_000);
        this.deserializer = new KafkaAvroDeserializer(registry);
        this.serializer = new KafkaAvroSerializer(registry,
                Map.of("schema.registry.url", schemaRegistryUrl, "auto.register.schemas", true));
    }

    GenericRecord decode(String rawTopic, byte[] value) {
        return (GenericRecord) deserializer.deserialize(rawTopic, value);
    }

    /** The row plus the CDC columns that bronze and the MERGE rely on, serialized for the clean topic. */
    byte[] encodeClean(String cleanTopic, GenericRecord row, String op, BigInteger version, long epoch,
                       long commitTsMs, RawRecord in) {
        Schema schema = cleanSchemas.computeIfAbsent(row.getSchema(), CleanRecordCodec::cleanSchemaFor);
        GenericRecord clean = new GenericData.Record(schema);
        for (Schema.Field f : row.getSchema().getFields()) {
            clean.put(f.name(), row.get(f.pos()));
        }
        clean.put("cdc_op", op);
        clean.put("cdc_version",
                DECIMAL.toBytes(new BigDecimal(version), VERSION_SCHEMA, VERSION_SCHEMA.getLogicalType()));
        clean.put("cdc_epoch", epoch);
        clean.put("src_commit_ts_utc", commitTsMs);
        clean.put("raw_partition", in.partition);
        clean.put("raw_offset", in.offset);
        return serializer.serialize(cleanTopic, clean);
    }

    static Schema cleanSchemaFor(Schema raw) {
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field f : raw.getFields()) {
            fields.add(new Schema.Field(f, f.schema()));          // Avro fields can't be shared between schemas
        }
        fields.add(new Schema.Field("cdc_op", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("cdc_version", VERSION_SCHEMA));
        fields.add(new Schema.Field("cdc_epoch", Schema.create(Schema.Type.LONG)));
        fields.add(new Schema.Field("src_commit_ts_utc", TIMESTAMP_SCHEMA));
        fields.add(new Schema.Field("raw_partition", Schema.create(Schema.Type.INT)));
        fields.add(new Schema.Field("raw_offset", Schema.create(Schema.Type.LONG)));
        String namespace = Objects.toString(raw.getNamespace(), "cdc") + ".clean";
        return Schema.createRecord(raw.getName(), raw.getDoc(), namespace, false, fields);
    }

    /** A field's value, or null when the record's schema doesn't have that field. */
    static Object field(GenericRecord row, String name) {
        Schema.Field f = row.getSchema().getField(name);
        return f == null ? null : row.get(f.pos());
    }
}
