package com.example.logical;

import com.example.logical.expressions.ColumnExprNode;
import com.example.logical.expressions.ConstantExprNode;
import com.example.logical.expressions.ExprNode;
import com.example.logical.expressions.FunctionExprNode;
import com.example.logical.functions.FunctionInfo;
import com.example.logical.functions.FunctionRegistry;
import com.example.logical.functions.GenericFunction;
import com.example.logical.objectparser.ObjectParser;
import com.example.parse.ASTNode;
import com.example.parse.SimpleParser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhishui
 */
public class ExpressionConversion {
//    here we only check
//    constant
//    caseExpresion
//    tableOrColumn
//    function
//    note: not include subquery

    public static ExprNode genExprNode(ASTNode expr, RowResolver rowResolver) throws SemanticException {

        // integer
        if (expr.getToken().getType() == SimpleParser.Number) {
            Number value = null;
            try {
                value = Double.valueOf(expr.getText());
                value = Long.valueOf(expr.getText());
                value = Integer.valueOf(expr.getText());
            } catch (NumberFormatException e) {
//                do nothing
            }

            if (value == null) {
                throw new SemanticException("wrong format of number");
            }
            return new ConstantExprNode(value, "Integer");

        } else if (expr.getToken().getType() == SimpleParser.StringLiteral) {
            String text = expr.getText();
            return new ConstantExprNode(text.trim(), "String");
        } else if (expr.getToken().getType() == SimpleParser.KW_TRUE) {
            return new ConstantExprNode(Boolean.TRUE, "Boolean");
        } else if (expr.getToken().getType() == SimpleParser.KW_FALSE) {
            return new ConstantExprNode(Boolean.FALSE, "Boolean");
        } else if (expr.getToken().getType() == SimpleParser.TOK_FUNCTION) {
            return processFunction(expr, rowResolver);
        } else if (expr.getToken().getType() == SimpleParser.TOK_TABLE_COL) {
            String colName = expr.getChild(0).getText();
            ColumnInfo columnInfo = rowResolver.get(null, colName);
            if (columnInfo == null) {
                throw new SemanticException("no corresponding column could found from child operator, please check your input");
            }
            return new ColumnExprNode(columnInfo);
        }

        return null;
    }

    public static ExprNode processFunction(ASTNode functionNode, RowResolver rowResolver) throws SemanticException {
        List<ExprNode> children = new ArrayList<ExprNode>();
        String functionName = functionNode.getChild(0).getText();
        // functionName is position 0
        for (int i = 0; i < functionNode.getChildCount(); i++) {
            children.add(genExprNode((ASTNode) functionNode.getChild(i), rowResolver));
        }

        if (children.contains(null)) {
            throw new SemanticException(String.format("there exists null parameter for function %s", functionName));
        }

        FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName);
        if (functionInfo == null) {
            throw new SemanticException(String.format("there does not exist function %s, please check", functionName));
        }

        ObjectParser[] objectParsers = new ObjectParser[children.size()];
        for (int i = 0; i < objectParsers.length; i++) {
            objectParsers[i] = children.get(i).getObjectParser();
        }

        GenericFunction genericFunction = functionInfo.getGenericFunction();
        ObjectParser resultO = genericFunction.initialize(objectParsers);
        return new FunctionExprNode(resultO, genericFunction, functionName, children);
    }


}
