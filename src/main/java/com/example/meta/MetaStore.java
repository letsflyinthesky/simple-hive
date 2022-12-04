package com.example.meta;

import com.example.logical.SimpleTable;


public class MetaStore {
    public SimpleTable getTable(String tableName) {
        SimpleTable table = new SimpleTable(tableName);
        return table;
    }
}
