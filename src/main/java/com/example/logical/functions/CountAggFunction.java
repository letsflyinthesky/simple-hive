package com.example.logical.functions;

import com.example.logical.aggregate.GroupByMode;
import com.example.logical.objectparser.LongObjectParser;

/**
 * @author zhishui
 */
public class CountAggFunction implements AggFunction {

    private GroupByMode mode;
    private LongObjectParser partialCountObjParser;

    @Override
    public void aggregate(Object buffer, Object[] params) {
        if (mode == GroupByMode.HASH || mode == GroupByMode.PARTIAL) {
            iterate(buffer, params);
        } else {
            merge(buffer, params[0]);
        }

    }

    public GroupByMode getMode() {
        return mode;
    }

    public void setMode(GroupByMode mode) {
        this.mode = mode;
    }

    @Override
    public String getReturnType() {
        return "LONG";
    }

    @Override
    public AggFunction copy() {
        CountAggFunction countAggFunction = new CountAggFunction();
        return countAggFunction;
    }

    public LongObjectParser getPartialCountObjParser() {
        return partialCountObjParser;
    }

    public void setPartialCountObjParser(LongObjectParser partialCountObjParser) {
        this.partialCountObjParser = partialCountObjParser;
    }

    @Override
    public Object evaluate(Object agg) {
        Object result = null;
        if (mode == GroupByMode.HASH  || mode == GroupByMode.MERGEPARTIAL) {
            result = terminatePartial(agg);
        } else {
            result = terminate(agg);
        }

        return result;
    }

    private Object terminate(Object agg) {
        CountAgg result = (CountAgg) agg;
        return result.value;
    }

    private Object terminatePartial(Object agg) {
        return terminatePartial(agg);
    }

    @Override
    public Object getAggBuffer() {
        CountAgg countAgg = new CountAgg();
        return countAgg;
    }


    private void iterate(Object buffer, Object[] params) {

        for (Object param : params) {
            if (param == null) {
                return;
            }
        }

        ((CountAgg) buffer).value++;


    }


    private void merge(Object buffer, Object partial) {
        if (partial != null) {
            CountAgg countAgg = (CountAgg) buffer;
            Long convert = partialCountObjParser.convert(partial);
            countAgg.value += convert;
        }

    }
}

class CountAgg {
    int value;
}
