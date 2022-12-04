package com.example.logical;

import java.io.Serializable;


public class ColumnInfo implements Serializable {
    private String internalName;
    private String typeName;
    private String tabAlias;
    private String alias;

    public ColumnInfo(String tabAlias, String alias, String typeName) {
        this.typeName = typeName;
        this.tabAlias = tabAlias;
        this.alias = alias;
    }

    public String getInternalName() {
        return internalName;
    }

    public void setInternalName(String internalName) {
        this.internalName = internalName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getTabAlias() {
        return tabAlias;
    }

    public void setTabAlias(String tabAlias) {
        this.tabAlias = tabAlias;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
