package com.example.logical.evaluator;

import com.example.logical.expressions.ColumnExprNode;
import com.example.logical.expressions.ConstantExprNode;
import com.example.logical.expressions.ExprNode;
import com.example.logical.expressions.FunctionExprNode;

/**
 * @author zhishui
 */
public class EvaluatorConverter {

    public static GenericEvaluator convert(ExprNode expr) {
        GenericEvaluator evaluator = null;
        if (expr instanceof ConstantExprNode) {
            evaluator = new ColumnExprEvaluator();
        } else if (expr instanceof FunctionExprNode) {
            evaluator = new FunctionExprEvaluator();
        } else if (expr instanceof ColumnExprNode) {
            evaluator = new ColumnExprEvaluator();
        }
        return evaluator;
    }

}
