package com.example.logical;

import com.example.parse.ASTNode;
import javafx.util.Pair;

import java.io.Serializable;
import java.util.*;

/**
 * @author zhishui
 */
public class RowResolver implements Serializable {

    //    for aggs
    private boolean aggResolver;
    private Map<String, ASTNode> expresionMap;

    // table : column : columnInfo
    private LinkedHashMap<String, LinkedHashMap<String, ColumnInfo>> internalMap;
    // internalName : columnInfo
    private Map<String, ColumnInfo> reverseInternalMap;
    // internalName : type
    private Map<String, String> typeMap;

    private List<ColumnInfo> columnInfos;

    public RowResolver() {
        internalMap = new LinkedHashMap<>();
        reverseInternalMap = new HashMap<>();
        typeMap = new HashMap<>();
        columnInfos = new ArrayList<>();
        expresionMap = new HashMap<>();
    }

    public boolean isAggResolver() {
        return aggResolver;
    }

    public void setAggResolver(boolean aggResolver) {
        this.aggResolver = aggResolver;
    }

    // for aggregation
    public void putExpression(ASTNode aggNode, ColumnInfo columnInfo) {
        String s = aggNode.toStringTree();
        expresionMap.put(s, aggNode);
        put("", s, columnInfo);
        columnInfos.add(columnInfo);
    }

    public ColumnInfo getExpression(ASTNode aggNode) throws SemanticException {
        return get("", aggNode.toStringTree());
    }

    public List<ColumnInfo> getColumnInfos() {
        return columnInfos;
    }

    public void put(String tableAlias, String columnAlias, ColumnInfo columnInfo) {
        LinkedHashMap<String, ColumnInfo> columnMap = internalMap.get(tableAlias);
        if (columnMap == null) {
            columnMap = new LinkedHashMap<>();
            internalMap.put(tableAlias, columnMap);
        }
        ColumnInfo cInfo = columnMap.get(columnAlias);
        if (cInfo != null) {
            System.out.println(String.format("there exists one same column alias with %s", columnAlias));
        }

        columnMap.put(columnAlias, columnInfo);
    }

    public ColumnInfo get(String tableName, String column) throws SemanticException {
        if (tableName == null) {
            boolean found = false;
            ColumnInfo result = null;
            for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> entry : internalMap.entrySet()) {
                LinkedHashMap<String, ColumnInfo> value = entry.getValue();
                for (Map.Entry<String, ColumnInfo> columns : value.entrySet()) {
                    if (column.equals(columns.getKey())) {
                        if (found) {
                            throw new SemanticException(String.format("column %s found in multiple table", column));
                        }
                        found = true;
                        result = columns.getValue();
                    }
                }
            }
            return result;
        } else {
            LinkedHashMap<String, ColumnInfo> columnMap = internalMap.get(tableName);
            if (columnMap != null) {
                return null;
            }
            return columnMap.get(column);
        }
    }

    public Map<String, ASTNode> getExpresionMap() {
        return expresionMap;
    }

    public void setExpresionMap(Map<String, ASTNode> expresionMap) {
        this.expresionMap = expresionMap;
    }

    public LinkedHashMap<String, LinkedHashMap<String, ColumnInfo>> getInternalMap() {
        return internalMap;
    }

    public void setInternalMap(LinkedHashMap<String, LinkedHashMap<String, ColumnInfo>> internalMap) {
        this.internalMap = internalMap;
    }

    public Map<String, ColumnInfo> getReverseInternalMap() {
        return reverseInternalMap;
    }

    public void setReverseInternalMap(Map<String, ColumnInfo> reverseInternalMap) {
        this.reverseInternalMap = reverseInternalMap;
    }

    public Map<String, String> getTypeMap() {
        return typeMap;
    }

    public void setTypeMap(Map<String, String> typeMap) {
        this.typeMap = typeMap;
    }
    public String[] reverseLookup(String internalName) {
        ColumnInfo columnInfo = reverseInternalMap.get(internalName);
        return new String[]{columnInfo.getTabAlias(), columnInfo.getAlias()};
    }

}

