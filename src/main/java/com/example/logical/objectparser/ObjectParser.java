package com.example.logical.objectparser;

import org.apache.hadoop.io.Writable;


public abstract class ObjectParser {

    public abstract Object convert(Object value);
    public abstract String serialize(Object... object);
    public abstract String getTypeName();


}
