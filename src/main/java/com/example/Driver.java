package com.example;

import com.example.logical.SemanticException;
import com.example.logical.SimpleAnalyzer;
import com.example.parse.ASTNode;
import com.example.physical.MapRedTask;

import java.io.IOException;
import java.util.List;


public class Driver {

    //完成sql
    public void compile(String command) throws SemanticException, IOException {
        Context context = new Context();
        context.setCommand(command);
        ASTNode tree = ParseUtil.parse(command, context);

        SimpleAnalyzer analyzer = new SimpleAnalyzer();
        //将analyzer返回的结果提交到MR上执行，将返回的结果打印出来。
        analyzer.analyze(tree, context);
        run(analyzer.getRootTasks());
    }

    public ASTNode findRootNotNullNode(ASTNode tree) {
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        return tree;
    }

    //    物理计划执行
    public void run(List<MapRedTask> rootTasks) throws IOException {
//      we think there are no dependencies in rootTasks
        for (MapRedTask task : rootTasks) {
            task.execute();

        }
    }

}
