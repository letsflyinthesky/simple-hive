package com.example.logical.expressions;

import com.example.logical.functions.GenericFunction;
import com.example.logical.objectparser.ObjectParser;

import java.util.List;

/**
 * @author zhishui
 */
public class FunctionExprNode extends ExprNode {
    private ObjectParser objectParser;
    private GenericFunction genericFunction;
    private String functionName;
    private List<ExprNode> children;

    public FunctionExprNode(ObjectParser objectParser, GenericFunction genericFunction,
                            String functionName, List<ExprNode> children) {
        super(objectParser.getTypeName());
        this.objectParser = objectParser;
        this.genericFunction = genericFunction;
        this.functionName = functionName;
        this.children = children;
    }

    @Override
    public boolean isSame(Object obj) {
        return false;
    }
}
