package com.example.logical.functions;

import com.example.logical.aggregate.GroupByMode;

/**
 * @author zhishui
 */
public interface AggFunction {

    void aggregate(Object buffer, Object[] params);

    Object evaluate(Object agg);

    Object getAggBuffer();

    GroupByMode getMode();

    void setMode(GroupByMode mode);

    String getReturnType();

    AggFunction copy();

}
