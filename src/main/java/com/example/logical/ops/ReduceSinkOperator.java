package com.example.logical.ops;

import com.example.logical.expressions.ConstantExprNode;
import com.example.logical.expressions.ExprNode;
import com.example.logical.objectparser.ObjectParser;
import com.example.logical.objectparser.StructObjectParser;
import com.example.serde.SimpleSerializer;
import javafx.beans.property.SimpleObjectProperty;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ReduceSinkOperator extends Operator {


    private List<ExprNode> reduceKeys;
    private List<ExprNode> values;
    private int tag;
    private ObjectParser[] inputObjectParsers;
    private boolean firstRow;
    private SimpleSerializer keySerializer;
    private SimpleSerializer valueSerializer;
    private StructObjectParser keyObjectParser;
    private StructObjectParser valueObjectParser;


    public int getTag() {
        return tag;
    }

    public void setTag(int tag) {
        this.tag = tag;
    }

    public ReduceSinkOperator(List<ExprNode> reduceKeys, List<ExprNode> values) {
        this.reduceKeys = reduceKeys;
        this.values = values;
    }

    @Override
    public void initialize() {
        keyObjectParser = new StructObjectParser();
        List<ObjectParser> keyObjectParsers = new ArrayList<>();
        for (int i = 0; i < reduceKeys.size(); i++) {
            keyObjectParsers.add(reduceKeys.get(i).getObjectParser());
        }
        keyObjectParser.setObjectParsers(keyObjectParsers);

        valueObjectParser = new StructObjectParser();
        List<ObjectParser> valueObjectParsers = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            valueObjectParsers.add(values.get(i).getObjectParser());
        }
        valueObjectParser.setObjectParsers(valueObjectParsers);

        keySerializer = new SimpleSerializer(keyObjectParser);
        valueSerializer = new SimpleSerializer(valueObjectParser);
    }

    public List<ExprNode> getReduceKeys() {
        return reduceKeys;
    }

    public void setReduceKeys(List<ExprNode> reduceKeys) {
        this.reduceKeys = reduceKeys;
    }

    public List<ExprNode> getValues() {
        return values;
    }

    public void setValues(List<ExprNode> values) {
        this.values = values;
    }


    @Override
    public void process(Object row, int tag) {
        ObjectParser inputObjectParser = inputObjectParsers[tag];
        Object[] objs = (Object[]) row;
        Object[] keyObjs = new Object[reduceKeys.size()];
        Object[] valueObjs = new Object[values.size()];
        System.arraycopy(objs, 0, keyObjs, 0, reduceKeys.size());
        System.arraycopy(objs, reduceKeys.size(), valueObjs, 0, values.size());

        Text key = keySerializer.serialize(keyObjs);
        Text value = valueSerializer.serialize(valueObjs);
        try {
            collect(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void collect(Text key, Text value) throws IOException {
        if (out != null) {
            out.collect(key, value);
        }
    }


}
