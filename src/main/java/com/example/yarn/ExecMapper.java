package com.example.yarn;

import com.example.io.IOContext;
import com.example.io.IOContextMap;
import com.example.physical.MapConf;
import com.example.physical.MapOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.ObjectInputStream;


public class ExecMapper extends MapReduceBase implements Mapper {

    private MapOperator mapOperator;
    private Configuration conf;
    private ExecMapperContext execMapperContext;

    @Override
    public void configure(JobConf job) {
        execMapperContext = new ExecMapperContext(job);
        MapConf mapConf = null;
        try {
            mapConf = getMapConf(job);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        mapOperator = new MapOperator(job);
        mapOperator.initialize();
        mapOperator.setExecCtx(execMapperContext);
        mapOperator.setParents(mapConf);
        mapOperator.initializeMapOperator(job);

    }


    private MapConf getMapConf(Configuration conf) throws IOException, ClassNotFoundException {
        String plan = conf.get("simple.hive.map.plan");
        Path planPath = new Path(plan);
        FileSystem fileSystem = planPath.getFileSystem(conf);
        FSDataInputStream open = fileSystem.open(planPath);
        long len = fileSystem.getFileStatus(planPath).getLen();

        ObjectInputStream objectInputStream = new ObjectInputStream(open);
        MapConf mapConf = (MapConf) objectInputStream.readObject();
        return mapConf;

    }

    @Override
    public void map(Object key, Object value, OutputCollector outputCollector, Reporter reporter) throws IOException {
        execMapperContext.resetRow();
        if (mapOperator.getOut() == null) {
            mapOperator.setOutputCollector(outputCollector);
        }

        mapOperator.process((Text)value);
    }
}

