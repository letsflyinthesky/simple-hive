package com.example.logical.evaluator;

import com.example.logical.objectparser.ObjectParser;

/**
 * @author zhishui
 */
public class FunctionExprEvaluator extends GenericEvaluator {
    @Override
    public Object eval(Object... params) {
        return params[0];
    }

    @Override
    public ObjectParser initialize(ObjectParser objectParser) {
        return objectParser;
    }
}
