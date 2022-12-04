package com.example.logical.functions;

import com.example.logical.expressions.ExprNode;
import com.example.logical.evaluator.GenericEvaluator;

import java.util.List;

/**
 * @author zhishui
 */
public class GenericFunctionInfo {
    private List<ExprNode> parameters;
    private GenericEvaluator evaluator;
    private String returnType;

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public List<ExprNode> getParameters() {
        return parameters;
    }

    public void setParameters(List<ExprNode> parameters) {
        this.parameters = parameters;
    }

    public GenericEvaluator getEvaluator() {
        return evaluator;
    }

    public void setEvaluator(GenericEvaluator evaluator) {
        this.evaluator = evaluator;
    }
}
