package org.apache.iceberg.streaming.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/** TimestampZonedFactory. */
public class TimestampZonedFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
        return new TimestampZoned();
    }
}
