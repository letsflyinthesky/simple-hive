package com.example.serde;

import com.example.logical.objectparser.*;
import com.google.common.base.Splitter;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SimpleDeserializer {

    private String typeName;
    private ObjectParser objectParser;

    public SimpleDeserializer(String typeName) {
        this.typeName = typeName;
        objectParser = getSimpleObjectParser(typeName);
    }

    public SimpleDeserializer(ObjectParser objectParser) {
        this.objectParser = objectParser;
    }

    private ObjectParser getSimpleObjectParser(String typeName) {
        if (typeName.contains(",")) {
            Iterator<String> iterator = Splitter.on(",").split(typeName).iterator();
            StructObjectParser structObjectParser = new StructObjectParser();
            List<ObjectParser> objectParsers = new ArrayList<>();
            while (iterator.hasNext()) {
                String next = iterator.next();
                objectParsers.add(getSimpleObjectParser(next));
            }
            structObjectParser.setObjectParsers(objectParsers);
            return structObjectParser;
        } else if (typeName.equalsIgnoreCase("INTEGER")){
            return new IntegerObjectParser();
        } else if (typeName.equalsIgnoreCase("LONG")) {
            return new LongObjectParser();
        } else if (typeName.equalsIgnoreCase("STRING")) {
            return new StringObjectParser();
        }
        throw new RuntimeException("not support type " + typeName);
    }

    public Object deserialize(Writable blob) {
        Text value = (Text) blob;
        Object result = objectParser.convert(value);
        return result;
    }

    private void parse() {

        if (objectParser instanceof StructObjectParser) {
            List<ObjectParser> objectParsers = ((StructObjectParser) objectParser).getObjectParsers();
            for (int i = 0; i < objectParsers.size(); i++) {


            }
        }
    }

}
