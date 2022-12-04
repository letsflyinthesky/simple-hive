package com.example.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;

import java.io.IOException;

/**
 * @author zhishui
 */
public class SimpleReader extends LineRecordReader {

    public SimpleReader(Configuration job, FileSplit split) throws IOException {
        super(job, split);
    }

    public void initIOContext(FileSplit split, Configuration job) {
        IOContext ioContext = IOContextMap.get(job);
        Path path = split.getPath();
        ioContext.setInputPath(path);
    }
}
