package com.example.logical.expressions;

import com.example.logical.ColumnInfo;

/**
 * @author zhishui
 */
public class ColumnExprNode extends ExprNode {

    private ColumnInfo columnInfo;

    public ColumnExprNode(ColumnInfo columnInfo) {
        super(columnInfo.getTypeName());
        this.columnInfo = columnInfo;
    }


    public ColumnInfo getColumnInfo() {
        return columnInfo;
    }

    public void setColumnInfo(ColumnInfo columnInfo) {
        this.columnInfo = columnInfo;
    }

    @Override
    public boolean isSame(Object obj) {
        return false;
    }
}

