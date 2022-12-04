package com.example.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @author zhishui
 */
public class SimpleInputFormat extends TextInputFormat {


    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        SimpleReader simpleReader = new SimpleReader(job, (FileSplit)split);
        simpleReader.initIOContext((FileSplit)split, job);
        return simpleReader;
    }
}
