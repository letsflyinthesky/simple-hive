package com.example.physical;

import com.example.logical.SemanticException;
import com.example.logical.objectparser.ObjectParser;
import com.example.logical.ops.*;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhishui
 */
public class TaskCompiler {

    //
    public void compile(List<MapRedTask> rootTasks, List<Operator> tableScanOps) throws SemanticException {
        Map<Operator, MapRedTask> map = new HashMap<>();
        //we need find top operator first
        for (Operator op : tableScanOps) {
            generateTaskTree(rootTasks, op, map);
        }
    }

    private void generateTaskTree(List<MapRedTask> rootTasks, Operator operator, Map<Operator, MapRedTask> map) throws SemanticException {
        if (operator instanceof TableScanOperator) {
            TableScanOperator scanOperator = (TableScanOperator) operator;
            MapRedTask mapRedTask = new MapRedTask();
            MapConf mapConf = mapRedTask.getMap();
            Path path = new Path(scanOperator.getPath());
            mapConf.addPathToOperator(path, scanOperator);

            map.put(scanOperator, mapRedTask);
            rootTasks.add(mapRedTask);
            for (Operator op : operator.getParents()) {
                generateTaskTree(rootTasks, op, map);
            }
        } else if (operator instanceof AggregateOperator) {
            Operator child = operator.getChildren().get(0);
            MapRedTask mapRedTask = map.get(child);
            map.put(operator, mapRedTask);
            for (Operator op : operator.getParents()) {
                generateTaskTree(rootTasks, op, map);
            }
        } else if (operator instanceof JoinOperator) {
            List<Operator> children = operator.getChildren();
            MapRedTask oldMapReadTask = null;
            MapRedTask newMapReadTask = null;
            for (int i = 0; i < children.size(); i++) {
                if (map.get(children.get(i)) == null) {
                    return;
                }
                if (i == 0) {
                    oldMapReadTask = map.get(children.get(i));
                } else {
                    newMapReadTask = map.get(children.get(i));
                }
            }
            mergeInput(oldMapReadTask, newMapReadTask);
            rootTasks.remove(newMapReadTask);
            map.put(operator, oldMapReadTask);
            return;
        } else if (operator instanceof FilterOperator) {
            Operator child = operator.getChildren().get(0);
            MapRedTask mapRedTask = map.get(child);
            map.put(operator, mapRedTask);
            for (Operator op : operator.getParents()) {
                generateTaskTree(rootTasks, op, map);
            }
        } else if (operator instanceof SelectOperator) {
            Operator child = operator.getChildren().get(0);
            MapRedTask mapRedTask = map.get(child);
            map.put(operator, mapRedTask);
            for (Operator op : operator.getParents()) {
                generateTaskTree(rootTasks, op, map);
            }
        } else if (operator instanceof ReduceSinkOperator) {
            Operator child = operator.getChildren().get(0);
            MapRedTask mapRedTask = map.get(child);
            if (mapRedTask.getReduce() == null) {
                genReducePlan(operator, mapRedTask);
            } else {
                //TODO
                splitPlan(operator, mapRedTask);
            }
            map.put(operator, mapRedTask);
        }
    }

    private void mergeInput(MapRedTask oldMapReadTask, MapRedTask newMapReadTask) {
        MapConf oldMapConf = oldMapReadTask.getMap();
        MapConf newMapConf = newMapReadTask.getMap();
        Map<Path, Operator> pathToOperator = newMapConf.getPathToOperator();
        Map<Path, ObjectParser> pathToParser = newMapConf.getPathToParser();

//        Set<Map.Entry<Path, Operator>> entries = pathToOperator.entrySet();
        for (Map.Entry<Path, Operator> entry : pathToOperator.entrySet()) {
            Path path = entry.getKey();
            Operator operator = entry.getValue();
            oldMapConf.addPathToOperator(path, operator);
        }

        for (Map.Entry<Path, ObjectParser> entry : pathToParser.entrySet()) {
            Path path = entry.getKey();
            ObjectParser objectParser = entry.getValue();
            oldMapConf.addPathToObjectParser(path, objectParser);
        }

    }


    private void splitPlan(Operator operator, MapRedTask mapRedTask) {
    }

    private void genReducePlan(Operator operator, MapRedTask mapRedTask) {
        RedConf redConf = new RedConf();
        mapRedTask.setReduce(redConf);
        Operator parent = operator.getParents().get(0);
        redConf.setReducer(parent);
    }


}

