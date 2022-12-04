package com.example.logical.ops;

import com.example.logical.SimpleTable;


public class TableScanOperator extends Operator {
    private SimpleTable table;

    public TableScanOperator() {
    }

    public TableScanOperator(SimpleTable table) {
        this.table = table;
    }

    public SimpleTable getTable() {
        return table;
    }

    public void setTable(SimpleTable table) {
        this.table = table;
    }

    public String getPath() {
        return table.getPath();
    }

    @Override
    public void process(Object row, int tag) {
        forward(row);
    }
}
