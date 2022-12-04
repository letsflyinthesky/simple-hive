package com.example.physical;

import com.example.io.IOContext;
import com.example.logical.objectparser.ObjectParser;
import com.example.logical.objectparser.StructObjectParser;
import com.example.logical.ops.Operator;
import com.example.logical.ops.TableScanOperator;
import com.example.serde.SimpleDeserializer;
import com.example.yarn.ExecMapperContext;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.*;

/**
 * @author zhishui
 */
public class MapOperator extends Operator {


    private List<Operator> topOps;
    private SimpleDeserializer deserializer;
    private Map<String, Map<Operator, MapOpCtx>> contexts;
    private MapOpCtx[] currentCtxs;
    private ExecMapperContext execCtx;
    private Configuration conf;

    public MapOperator(Configuration conf) {
        contexts = new HashMap<>();
        topOps = new ArrayList<>();
        this.conf = conf;
    }

    public ExecMapperContext getExecCtx() {
        return execCtx;
    }

    public void setExecCtx(ExecMapperContext execCtx) {
        this.execCtx = execCtx;
    }

    @Override
    public void process(Object row, int tag) {

    }

    public void setParents(MapConf mapConf) {
        List<Operator> parents = new ArrayList<>();
        for (Map.Entry<Path, Operator> entry : mapConf.getPathToOperator().entrySet()) {
            Path inputFile = entry.getKey();
            Operator parentOperator = entry.getValue();
            TableScanOperator operator = (TableScanOperator) parentOperator;
            List<String> fieldTypes = operator.getTable().getFieldTypes();
            List<ObjectParser> objParers = new ArrayList<>();
            Map<Operator, MapOpCtx> opCtxMap = contexts.get(inputFile.toString());
            try {
                FileSystem fileSystem = inputFile.getFileSystem(conf);
                inputFile = inputFile.makeQualified(fileSystem);
            } catch (IOException e) {
            }
            if (opCtxMap == null) {
                contexts.put(inputFile.toString(), opCtxMap = new LinkedHashMap<>());
            }
            MapOpCtx opCtx = new MapOpCtx(new SimpleDeserializer(Joiner.on(",").join(fieldTypes)), parentOperator);
            opCtxMap.put(parentOperator, opCtx);

            if (!parents.contains(parentOperator)) {
                parentOperator.getChildren().add(this);
                parents.add(parentOperator);
            }
        }

        this.setParents(parents);
    }



    public void initializeMapOperator(JobConf job) {
    }

    public void process(Writable value) {
        ExecMapperContext execCtx = getExecCtx();
        if (execCtx != null && execCtx.inputFileChanged()) {
            cleanUpInputFileChangedOp();
        }

        for (MapOpCtx ctx : currentCtxs) {
            Object o = ctx.readRow(value);
            ctx.forward(o);
        }
    }



    public void cleanUpInputFileChangedOp() {
        Path inputPath = getExecCtx().getCurrentPath();
        Map<Operator, MapOpCtx> opCtxMap = contexts.get(inputPath.toString());
        currentCtxs = opCtxMap.values().toArray(new MapOpCtx[opCtxMap.values().size()]);
    }
}

class MapOpCtx {
    SimpleDeserializer deserializer;
    Operator op;

    public MapOpCtx(SimpleDeserializer deserializer, Operator op) {
        this.deserializer = deserializer;
        this.op = op;
    }

    public Object readRow(Writable value) {
        Object deserialize = deserializer.deserialize(value);
        return deserialize;
    }

    public void forward(Object row) {
        op.process(row, 0);
    }
}