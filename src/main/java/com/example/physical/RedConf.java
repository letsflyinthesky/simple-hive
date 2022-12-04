package com.example.physical;

import com.example.logical.ops.Operator;


public class RedConf extends BaseConf {
    private Operator reducer;

    public Operator getReducer() {
        return reducer;
    }

    public void setReducer(Operator reducer) {
        this.reducer = reducer;
    }
}
