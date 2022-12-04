package com.example.yarn;

import com.example.io.IOContext;
import com.example.io.IOContextMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;


public class ExecMapperContext {

    private Path currentPath;
    private Path lastInputPath;
    private boolean inputFileChecked;
    private IOContext context;

    public ExecMapperContext(JobConf job) {
        this.context  = IOContextMap.get(job);
    }

    public boolean inputFileChanged() {
        if (!inputFileChecked) {
            currentPath = this.context.getInputPath();
            inputFileChecked = true;
        }
        return lastInputPath == null || lastInputPath.equals(currentPath);
    }

    public Path getCurrentPath() {
        return currentPath;
    }

    public void resetRow() {
        lastInputPath = currentPath;
        inputFileChecked = false;
    }

}
