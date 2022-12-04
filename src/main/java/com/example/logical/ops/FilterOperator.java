package com.example.logical.ops;

import com.example.logical.evaluator.EvaluatorConverter;
import com.example.logical.evaluator.GenericEvaluator;
import com.example.logical.expressions.ExprNode;


public class FilterOperator extends Operator {
    private ExprNode predicate;
    private GenericEvaluator evaluator;

    public FilterOperator(ExprNode predicate) {
        this.predicate = predicate;
    }

    @Override
    public void initialize() {
        evaluator = EvaluatorConverter.convert(predicate);
    }

    @Override
    public void process(Object row, int tag) {
        Boolean result = (Boolean) evaluator.eval(row);
        if (result.equals(Boolean.TRUE)) {
            forward(row);
        }

    }
}
