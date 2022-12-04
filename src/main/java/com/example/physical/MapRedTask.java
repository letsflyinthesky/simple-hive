package com.example.physical;

import com.example.io.SimpleInputFormat;
import com.example.logical.ops.Operator;
import com.example.yarn.ExecMapper;
import com.example.yarn.ExecReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * @author zhishui
 */
public class MapRedTask {

    private MapConf map;
    private RedConf reduce;
    private MapRedTask dependencyTask;
    private JobConf job;

    public MapRedTask() {
        map = new MapConf();
        reduce = new RedConf();
        job = new JobConf(MapRedTask.class);
    }

    public MapRedTask getDependencyTask() {
        return dependencyTask;
    }

    public void setDependencyTask(MapRedTask dependencyTask) {
        this.dependencyTask = dependencyTask;
    }

    public MapConf getMap() {
        return map;
    }

    public void setMap(MapConf map) {
        this.map = map;
    }

    public RedConf getReduce() {
        return reduce;
    }

    public void setReduce(RedConf reduce) {
        this.reduce = reduce;
    }

    public void execute() throws IOException {
        // run mapred task in same jvm while mapred task sumbmit to yarn
        job.setMapperClass(ExecMapper.class);
        job.setInputFormat(SimpleInputFormat.class);

//
        if (false) {
            job.setNumReduceTasks(1);
            job.setReducerClass(ExecReducer.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String outputStr = "/simple-hive/output";
        Path path = new Path(outputStr);
        FileSystem fileSystem = path.getFileSystem(job);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        job.set("mapreduce.output.fileoutputformat.outputdir", "/simple-hive/output");

        setBaseWork(job, map, "map.xml");
        setBaseWork(job, reduce, "reduce.xml");

        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        Path[] dataPaths = new Path[inputPaths.length + map.getPathToOperator().keySet().size()];
        System.arraycopy(inputPaths, 0, dataPaths, 0, inputPaths.length);
        int offset = inputPaths.length;
        for (Map.Entry<Path, Operator> entry : map.getPathToOperator().entrySet()) {
            dataPaths[offset++] = entry.getKey();
        }
        FileInputFormat.setInputPaths(job, dataPaths);

        JobClient jobClient = new JobClient(job);
        RunningJob runningJob = jobClient.submitJob(job);
        runningJob.waitForCompletion();
    }

    public void setBaseWork(Configuration conf, BaseConf work, String name) throws IOException {
        if (name.equalsIgnoreCase("map.xml")) {
            conf.setBoolean("has.map.work", Boolean.TRUE);
            conf.set("simple.hive.map.plan", "/simple-hive/" + name);
        } else if (name.equalsIgnoreCase("reduce.xml")) {
            conf.setBoolean("has.map.reduce", Boolean.TRUE);
            conf.set("simple.hive.reduce.plan", "/simple-hive/" + name);
        }
        Path path = new Path("/simple-hive/" + name);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataOutputStream fsDataOutputStream = null;
        if (!fileSystem.exists(path)) {
            fsDataOutputStream = fileSystem.create(path);
        } else {
            fileSystem.delete(path);
            fsDataOutputStream = fileSystem.create(path);
        }
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fsDataOutputStream);
        objectOutputStream.writeObject(work);
        objectOutputStream.flush();
        objectOutputStream.close();
        fsDataOutputStream.close();
    }
}
