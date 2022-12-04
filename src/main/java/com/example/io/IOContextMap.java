package com.example.io;

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ConcurrentHashMap;



// track file changed
public class IOContextMap {

    private static final ConcurrentHashMap<String, IOContext> globalMap =
            new ConcurrentHashMap<String, IOContext>();

    public static IOContext get(Configuration conf) {
        String inputName = conf.get("iocontext.input.name");
        if (inputName == null) {
            inputName = "";
        }
        IOContext ioContext = globalMap.get(inputName);
        if (ioContext != null) {
            return ioContext;
        }
        ioContext = new IOContext();
        globalMap.putIfAbsent(inputName, ioContext);
        return ioContext;
    }


}
