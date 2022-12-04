package com.example.logical;

import com.example.logical.ops.Operator;
import com.example.parse.ASTNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class QueryBlockJoinTree {

    private String[] leftAliases;
    private String[] rightAliases;
    private QueryBlockJoinTree joinSrc;
    private Map<String, Operator> aliasToOps;

    private String[] baseSrc;
    private List<List<ASTNode>> filters;
    private List<List<ASTNode>> expressions;
    private JoinCond[] joinConds;


    public String[] getLeftAliases() {
        return leftAliases;
    }

    public void setLeftAliases(String[] leftAliases) {
        this.leftAliases = leftAliases;
    }

    public String[] getRightAliases() {
        return rightAliases;
    }

    public void setRightAliases(String[] rightAliases) {
        this.rightAliases = rightAliases;
    }

    public JoinCond[] getJoinConds() {
        return joinConds;
    }

    public void setJoinConds(JoinCond[] joinConds) {
        this.joinConds = joinConds;
    }

    public List<List<ASTNode>> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<List<ASTNode>> expressions) {
        this.expressions = expressions;
    }

    public List<List<ASTNode>> getFilters() {
        return filters;
    }

    public void setFilters(List<List<ASTNode>> filters) {
        this.filters = filters;
    }

    private List<List<ASTNode>> joinPredicates;

    public QueryBlockJoinTree() {
        aliasToOps = new HashMap<>();
    }


    public QueryBlockJoinTree getJoinSrc() {
        return joinSrc;
    }

    public void setJoinSrc(QueryBlockJoinTree joinSrc) {
        this.joinSrc = joinSrc;
    }

    public String[] getBaseSrc() {
        return baseSrc;
    }

    public void setBaseSrc(String[] baseSrc) {
        this.baseSrc = baseSrc;
    }
}
