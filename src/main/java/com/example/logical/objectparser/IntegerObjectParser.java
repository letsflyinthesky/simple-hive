package com.example.logical.objectparser;

import org.apache.hadoop.io.WritableUtils;

/**
 * @author zhishui
 */
public class IntegerObjectParser extends ObjectParser {

    private String typeName = "INTEGER";

    @Override
    public Object convert(Object value) {
        return Integer.valueOf(value.toString());
    }

    @Override
    public String serialize(Object... object) {
        return ((Integer)object[0]).toString();
    }

    public String getTypeName() {
        return typeName;
    }
}
