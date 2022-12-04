package com.example.logical.ops;

import com.example.logical.objectparser.ObjectParser;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhishui
 */
public abstract class Operator implements Serializable {

    protected List<Operator> children;
    protected List<Operator> parents;
    protected ObjectParser objectParser;
    protected int[] parentOperatorsTag;
    protected OutputCollector out;

    public Operator() {
        children = new ArrayList<>();
        parents = new ArrayList<>();
    }

    public OutputCollector getOut() {
        return out;
    }

    public void setOutputCollector(OutputCollector out) {
        this.out = out;

        for (Operator op : parents) {
            op.setOutputCollector(out);
        }
    }

    public List<Operator> getChildren() {
        return children;
    }

    public void setChildren(List<Operator> children) {
        this.children = children;
    }

    public List<Operator> getParents() {
        return parents;
    }

    public void setParents(List<Operator> parents) {
        this.parents = parents;
    }

    public abstract void process(Object row, int tag);

    public void initialize() {

    }


    public void forward(Object row) {
        for (int i = 0; i < parents.size(); i++) {
            Operator operator = parents.get(i);
            operator.process(row, i);
        }
        if (parents.isEmpty()) {
            System.out.println(Arrays.toString((Object[])row));
        }
    }
}
