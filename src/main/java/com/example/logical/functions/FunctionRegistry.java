package com.example.logical.functions;

import com.example.logical.aggregate.GroupByMode;
import com.example.logical.evaluator.GenericEvaluator;
import com.example.logical.expressions.ExprNode;
import com.example.logical.objectparser.ObjectParser;

import java.util.List;

/**
 * @author zhishui
 */
public class FunctionRegistry {

    public static FunctionInfo getFunctionInfo(String functionName) {
        return null;
    }


    public static AggFunction getAggFunction(String aggName, List<ObjectParser> argObjParser, GroupByMode mode) {

        if (aggName.equalsIgnoreCase("count")) {
            CountAggFunction countAggFunction = new CountAggFunction();
            countAggFunction.setMode(mode);
            return countAggFunction;
        }
        throw new RuntimeException("there is no agg function found");
    }

    public static AggFunction getAggFunctionWithNodes(String aggName, List<ExprNode> exprNodes, GroupByMode mode) {


        if (aggName.equalsIgnoreCase("count")) {
            CountAggFunction countAggFunction = new CountAggFunction();
            countAggFunction.setMode(mode);
            return countAggFunction;
        }
        throw new RuntimeException("there is no agg function found");
    }

}
