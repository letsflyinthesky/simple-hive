package com.example.physical;

import com.example.logical.objectparser.ObjectParser;
import com.example.logical.ops.Operator;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;


public class MapConf extends BaseConf {

    private static final long serialVersionUID = 1L;

    public Map<Path, ObjectParser> pathToParser;
    public Map<Path, Operator> pathToOperator;

    public MapConf() {
        pathToOperator = new HashMap<>();
        pathToParser = new HashMap<>();
    }

    public Map<Path, Operator> getPathToOperator() {
        return pathToOperator;
    }

    public void setPathToOperator(Map<Path, Operator> pathToOperator) {
        this.pathToOperator = pathToOperator;
    }

    public void addPathToOperator(Path path, Operator operator) {
        pathToOperator.put(path, operator);
    }

    public void addPathToObjectParser(Path path, ObjectParser objectParser) {
        pathToParser.put(path, objectParser);
    }

    public Map<Path, ObjectParser> getPathToParser() {
        return pathToParser;
    }

    public void setPathToParser(Map<Path, ObjectParser> pathToParser) {
        this.pathToParser = pathToParser;
    }
}
