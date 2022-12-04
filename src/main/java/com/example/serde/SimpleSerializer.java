package com.example.serde;

import com.example.logical.objectparser.ObjectParser;
import org.apache.hadoop.io.Text;


public class SimpleSerializer {

    private String typeName;
    private ObjectParser objectParser;

    public SimpleSerializer(ObjectParser objectParsers) {
        this.objectParser = objectParsers;
    }

    public Text serialize(Object... obj) {
        String serialize = objectParser.serialize(obj);
        return new Text(serialize);
    }
}
