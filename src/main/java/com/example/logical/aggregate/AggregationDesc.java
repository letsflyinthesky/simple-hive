package com.example.logical.aggregate;

import com.example.logical.expressions.ExprNode;
import com.example.logical.evaluator.GenericEvaluator;
import com.example.logical.functions.AggFunction;

import java.util.List;

/**
 * @author zhishui
 */
public class AggregationDesc {
    private String aggregationName;
    private List<ExprNode> parameters;
    private GroupByMode mode;
    private AggFunction aggFunction;

    public AggregationDesc(String aggregationName, List<ExprNode> parameters,
                           AggFunction aggFunction, GroupByMode mode) {
        this.aggregationName = aggregationName;
        this.parameters = parameters;
        this.aggFunction = aggFunction;
        this.mode = mode;
    }

    public AggFunction getAggFunction() {
        return aggFunction;
    }

    public void setAggFunction(AggFunction aggFunction) {
        this.aggFunction = aggFunction;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public void setAggregationName(String aggregationName) {
        this.aggregationName = aggregationName;
    }

    public List<ExprNode> getParameters() {
        return parameters;
    }

    public void setParameters(List<ExprNode> parameters) {
        this.parameters = parameters;
    }

    public GroupByMode getMode() {
        return mode;
    }

    public void setMode(GroupByMode mode) {
        this.mode = mode;
    }
}
