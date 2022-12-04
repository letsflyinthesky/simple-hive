package com.example.logical.expressions;

import com.example.logical.objectparser.IntegerObjectParser;
import com.example.logical.objectparser.LongObjectParser;
import com.example.logical.objectparser.ObjectParser;
import com.example.logical.objectparser.StringObjectParser;

import java.io.Serializable;

/**
 * @author zhishui
 */
public abstract class ExprNode implements Serializable {

    private String typeStr;

    public ExprNode(String typeStr) {
        this.typeStr = typeStr;
    }

    public ObjectParser getObjectParser() {
        if (typeStr.equalsIgnoreCase("String")) {
            return new StringObjectParser();
        } else if (typeStr.equalsIgnoreCase("Integer")) {
            return new IntegerObjectParser();
        } else if (typeStr.equalsIgnoreCase("Long")) {
            return new LongObjectParser();
        } else {
            throw new RuntimeException(String.format("does not support type %s", typeStr));
        }
    }

    public abstract boolean isSame(Object obj);
}
