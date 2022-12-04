package com.example.logical.objectparser;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhishui
 */
public class StructObjectParser extends ObjectParser {

    private String typeName;
    private List<ObjectParser> objectParsers;

    public StructObjectParser() {
    }

    public List<ObjectParser> getObjectParsers() {
        return objectParsers;
    }

    public void setObjectParsers(List<ObjectParser> objectParsers) {
        this.objectParsers = objectParsers;
    }

    @Override
    public Object convert(Object value) {
        ArrayList<String> values = Lists.newArrayList(Splitter.on(",").split(value.toString()).iterator());
        Object[] convertedValues = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            convertedValues[i] = objectParsers.get(i).convert(values.get(i));
        }
        return convertedValues;
    }

    @Override
    public String serialize(Object... object) {
        String join = Joiner.on(",").join(object);
        return join;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }
}
