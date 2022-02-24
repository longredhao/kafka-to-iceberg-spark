package org.apache.iceberg.streaming.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

/** TimestampZoned represents a date and time in TimestampZoned String */
public class TimestampZoned extends LogicalType {
    public static final String TIMESTAMP_ZONED = "timestamp-zoned";

    TimestampZoned() {
        super(TIMESTAMP_ZONED);
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
        if (schema.getType() != Schema.Type.STRING) {
            throw new IllegalArgumentException(
                    "Timestamp (zoned) can only be used with an underlying string type");
        }
    }
}
