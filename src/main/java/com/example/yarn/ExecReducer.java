package com.example.yarn;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;


public class ExecReducer extends MapReduceBase implements Reducer {

    @Override
    public void reduce(Object o, Iterator iterator, OutputCollector outputCollector, Reporter reporter) throws IOException {

    }
}
