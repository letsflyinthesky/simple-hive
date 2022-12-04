package com.example.logical;

import com.example.parse.ASTNode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public class QueryBlockParseInfo {
    public boolean isSubQuery;
    private String alias;
    private ASTNode joinExpr;
    private HashMap<String, ASTNode> aliasToSrc;

    private LinkedHashMap<String, ASTNode> aggregationExprs;
    private ASTNode whereExpr;
    private ASTNode groupBy;


    public Map<ASTNode, String> getExprToColumnAlias() {
        return exprToColumnAlias;
    }

    public void setExprToColumnAlias(ASTNode astNode, String alias) {
        this.exprToColumnAlias.put(astNode, alias);
    }

    private Map<ASTNode, String> exprToColumnAlias;

    public QueryBlockParseInfo() {
        aliasToSrc = new HashMap<>();
        exprToColumnAlias = new HashMap<>();
    }


    public ASTNode getWhereExpr() {
        return whereExpr;
    }

    public void setWhereExpr(ASTNode whereExpr) {
        this.whereExpr = whereExpr;
    }

    public ASTNode getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(ASTNode groupBy) {
        this.groupBy = groupBy;
    }

    public ASTNode getSelectExpr() {
        return selectExpr;
    }

    public void setSelectExpr(ASTNode selectExpr) {
        this.selectExpr = selectExpr;
    }

    private ASTNode selectExpr;

    public boolean isSubQuery() {
        return isSubQuery;
    }

    public void setSubQuery(boolean subQuery) {
        isSubQuery = subQuery;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public ASTNode getJoinExpr() {
        return joinExpr;
    }

    public void setJoinExpr(ASTNode joinExpr) {
        this.joinExpr = joinExpr;
    }

    public HashMap<String, ASTNode> getAliasToSrc() {
        return aliasToSrc;
    }

    public void setAliasToSrc(HashMap<String, ASTNode> aliasToSrc) {
        this.aliasToSrc = aliasToSrc;
    }

    public void setExprToColumnAlias(Map<ASTNode, String> exprToColumnAlias) {
        this.exprToColumnAlias = exprToColumnAlias;
    }

    public LinkedHashMap<String, ASTNode> getAggregationExprs() {
        return aggregationExprs;
    }

    public void setAggregationExprs(LinkedHashMap<String, ASTNode> aggregationExprs) {
        this.aggregationExprs = aggregationExprs;
    }
}
