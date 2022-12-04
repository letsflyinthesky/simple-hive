package com.example.logical.objectparser;

/**
 * @author zhishui
 */
public class StringObjectParser extends ObjectParser {

    private String typeName = "STRING";

    @Override
    public Object convert(Object value) {
        return value.toString();
    }

    @Override
    public String serialize(Object... object) {
        return object[0].toString();
    }


    public String getTypeName() {
        return typeName;
    }
}
