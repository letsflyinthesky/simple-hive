package com.example.io;

import org.apache.hadoop.fs.Path;


public class IOContext {

    private Path inputPath;
    public Path getInputPath() {
        return inputPath;
    }

    public void setInputPath(Path inputPath) {
        this.inputPath = inputPath;
    }
}

