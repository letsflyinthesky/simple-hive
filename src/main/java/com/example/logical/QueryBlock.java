package com.example.logical;

import java.util.HashMap;


public class QueryBlock {


    private String id;
    private HashMap<String, String> aliasToTabs;
    private HashMap<String, String> aliasToSubQuery;

    private QueryBlockParseInfo qbParseInfo;
    private QueryBlockMetaData qbMetaData;
    private QueryBlockJoinTree qbJoinTree;

    public QueryBlock() {
        this.aliasToTabs = new HashMap<>();
        this.aliasToSubQuery = new HashMap<>();
        this.qbParseInfo = new QueryBlockParseInfo();
        this.qbMetaData = new QueryBlockMetaData();
    }

    public HashMap<String, String> getAliasToTabs() {
        return aliasToTabs;
    }

    public void setAliasToTabs(String alias, String tableName) {
        this.aliasToTabs.put(alias, tableName) ;
    }

    public HashMap<String, String> getAliasToSubQuery() {
        return aliasToSubQuery;
    }

    public void setAliasToSubQuery(HashMap<String, String> aliasToSubQuery) {
        this.aliasToSubQuery = aliasToSubQuery;
    }

    public QueryBlockParseInfo getQbParseInfo() {
        return qbParseInfo;
    }

    public void setQbParseInfo(QueryBlockParseInfo qbParseInfo) {
        this.qbParseInfo = qbParseInfo;
    }

    public QueryBlockMetaData getQbMetaData() {
        return qbMetaData;
    }

    public void setQbMetaData(QueryBlockMetaData qbMetaData) {
        this.qbMetaData = qbMetaData;
    }

    public QueryBlockJoinTree getQbJoinTree() {
        return qbJoinTree;
    }

    public void setQbJoinTree(QueryBlockJoinTree qbJoinTree) {
        this.qbJoinTree = qbJoinTree;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
