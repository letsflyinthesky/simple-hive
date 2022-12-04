package com.example.logical;

import java.util.HashMap;
import java.util.Map;


public class QueryBlockMetaData {

    private Map<String, SimpleTable> aliasToTable;

    public QueryBlockMetaData() {
        aliasToTable = new HashMap<>();
    }

    public Map<String, SimpleTable> getAliasToTable() {
        return aliasToTable;
    }

    public void setAliasToTable(String alias, SimpleTable table) {
        this.aliasToTable.put(alias, table);
    }
}
