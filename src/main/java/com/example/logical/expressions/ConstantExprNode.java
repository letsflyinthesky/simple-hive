package com.example.logical.expressions;

/**
 * @author zhishui
 */
public class ConstantExprNode extends ExprNode {

    private Object value;


    public ConstantExprNode(Object value, String typeStr) {
        super(typeStr);
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public boolean isSame(Object obj) {
        return false;
    }
}
