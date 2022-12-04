package com.example.logical;

import org.apache.hadoop.mapred.InputFormat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


//main contains fields and types
public class SimpleTable implements Serializable {
//

    private List<String> fieldNames;
    private List<String> fieldTypes;
    private String tableName;

    private String inputFormat;
    private String path;

    public SimpleTable(String tableName) {
        this.tableName = tableName;
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        //将inputFormat转换为对应的class
        return null;
    }


    public List<String> getFieldNames() {
        List<String> fields = new ArrayList<>();
        if (tableName.equals("my_test")) {
            fields.add("id");
            fields.add("name");
            fields.add("age");
        } else if (tableName.equals("my_test2")) {
            fields.add("id");
            fields.add("depart");
            fields.add("age");
        }
        this.fieldNames = fields;
        return fields;
    }

    public List<String> getFieldTypes() {
        List<String> fieldTypes = new ArrayList<>();
        if (tableName.equals("my_test")) {
            fieldTypes.add("Integer");
            fieldTypes.add("string");
            fieldTypes.add("long");
        } else if (tableName.equals("my_test2")) {
            fieldTypes.add("long");
            fieldTypes.add("string");
            fieldTypes.add("Integer");
        }
        this.fieldTypes = fieldTypes;
        return this.fieldTypes;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPath() {
        if (tableName.equals("my_test")) {
            path = "/simple-hive/my_test.txt";
        } else if (tableName.equals("my_test2")) {
            path = "/simple-hive/my_test2.txt";
        }
        return path;
    }
}
