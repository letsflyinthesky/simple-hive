package com.example.logical.objectparser;

import org.apache.hadoop.io.Writable;

/**
 * @author zhishui
 */
public class LongObjectParser extends ObjectParser {

    private String typeName = "LONG";

    @Override
    public Long convert(Object value) {
        return Long.valueOf(value.toString());
    }

    @Override
    public String serialize(Object... object) {
        return ((Long)object[0]).toString();
    }

    public String getTypeName() {
        return typeName;
    }
}
