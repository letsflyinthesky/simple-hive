package com.example.logical.ops;

import com.example.logical.evaluator.EvaluatorConverter;
import com.example.logical.evaluator.GenericEvaluator;
import com.example.logical.expressions.ExprNode;
import com.example.logical.objectparser.ObjectParser;

import java.util.ArrayList;
import java.util.List;


public class SelectOperator extends Operator {
    private List<ExprNode> projectNodes;
    private List<GenericEvaluator> evaluators;
    private Object[] output;
    private ObjectParser outputObjectParser;


    public SelectOperator(List<ExprNode> projectNodes) {
        this.projectNodes = projectNodes;
    }


    @Override
    public void initialize() {
        evaluators = new ArrayList<>();
        for (int i = 0; i < projectNodes.size(); i++) {
            GenericEvaluator evaluator = EvaluatorConverter.convert(projectNodes.get(i));
            evaluators.add(evaluator);
        }
        // TODO
        outputObjectParser = null;

    }

    @Override
    public void process(Object row, int tag) {
//        for (int i = 0; i < evaluators.size(); i++) {
//            output[i] = evaluators.get(i).eval(row);
//        }
        Object[] objs = (Object[]) row;
        output = new Object[projectNodes.size()];
        for (int i = 0; i < projectNodes.size(); i++) {
            output[i] = projectNodes.get(i).getObjectParser().convert(objs[i]);
        }
        forward(output);
    }
}
