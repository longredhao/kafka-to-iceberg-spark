package org.apache.iceberg.streaming.exception;

public class SchemaChangedException extends Exception{

    public SchemaChangedException(){}

    public SchemaChangedException(String message){
        super(message);
    }
    public SchemaChangedException(String message, Throwable cause) {
        super(message, cause);
    }

}
