package com.example.logical.ops;

import com.example.logical.evaluator.EvaluatorConverter;
import com.example.logical.evaluator.GenericEvaluator;
import com.example.logical.expressions.ExprNode;
import com.example.logical.aggregate.AggregationDesc;
import com.example.logical.functions.AggFunction;
import com.example.logical.objectparser.ObjectParser;

import java.util.BitSet;
import java.util.List;

/**
 * @author zhishui
 */
public class AggregateOperator extends Operator {
    private List<ExprNode> groupbyKeys;
    private List<BitSet> groupingSets;
    private List<AggregationDesc> aggregations;


    private GenericEvaluator[] keyEvaluators;
    private ObjectParser[] keyObjectParser;

    private GenericEvaluator[][] aggParameterFields;
    private Object[][] aggParameters;

    private Object[] currentKeys;
    private Object[] aggregationBuffers;

    private AggFunction[] aggFunctions;
    private Object[] forwardCache;

    @Override
    public void initialize() {
        int keySize = groupbyKeys.size();
        keyEvaluators = new GenericEvaluator[keySize];

        ObjectParser objParser = objectParser;

        for (int i = 0; i < keySize; i++) {
            keyEvaluators[i] = EvaluatorConverter.convert(groupbyKeys.get(i));
            keyObjectParser[i] = keyEvaluators[i].initialize(objParser);
        }

        aggParameterFields = new GenericEvaluator[aggregations.size()][];

        for (int i = 0; i < aggParameterFields.length; i++) {
            AggregationDesc aggregationDesc = aggregations.get(i);
            List<ExprNode> parameters = aggregationDesc.getParameters();
            GenericEvaluator[] genericEvaluators = new GenericEvaluator[parameters.size()];
            ObjectParser[] objectParsers = new ObjectParser[parameters.size()];
            for (int j = 0; j < parameters.size(); j++) {
                aggParameterFields[i][j] = EvaluatorConverter.convert(parameters.get(j));
            }
        }

        aggFunctions = new AggFunction[aggregations.size()];
        for (int i = 0; i < aggFunctions.length; i++) {
            AggregationDesc aggregationDesc = aggregations.get(i);
            aggFunctions[i] = aggregationDesc.getAggFunction();
        }

        aggregationBuffers = newAggregationBuffers();

    }

    private Object[] newAggregationBuffers() {
        Object[] aggBuffers = new Object[aggFunctions.length];
        for (int i = 0; i < aggFunctions.length; i++) {
            aggBuffers[i] = aggFunctions[i].getAggBuffer();
        }
        return aggBuffers;
    }

    public AggregateOperator(List<AggregationDesc> aggregations, List<ExprNode> groupbyKeys,
                             List<BitSet> groupingSets) {
        this.groupbyKeys = groupbyKeys;
        this.groupingSets = groupingSets;
        this.aggregations = aggregations;
    }

    @Override
    public void process(Object row, int tag) {
        Object[] newKeys = processCurrentKeys(row);
        processAggr(row, objectParser, newKeys);
    }

    private Object[] processCurrentKeys(Object row) {
        Object[] keys = new Object[groupbyKeys.size()];
        for (int i = 0; i < groupbyKeys.size(); i++) {
            keys[i] = keyEvaluators[i].eval(row);
        }
        return keys;
    }

    private void processAggr(Object row, ObjectParser objParser, Object[] newKeys) {
        boolean keysEqual = (currentKeys != null && newKeys != null) ?
                newKeys.equals(currentKeys) : false;

        if (currentKeys == null || !keysEqual) {
            currentKeys = newKeys;

            //forward?
            if (forwardCache == null) {
                forwardCache = new Object[groupbyKeys.size() + aggregations.size()];
            }
            for (int i = 0; i < groupbyKeys.size(); i++) {
                forwardCache[i] = currentKeys[i];
            }
            for (int i = 0; i < aggregations.size(); i++) {
                forwardCache[groupbyKeys.size() + i] = aggFunctions[i].evaluate(aggregationBuffers[i]);
            }

            forward(forwardCache);
        }
        if (!keysEqual) {
            for (int i = 0; i < aggregationBuffers.length; i++) {
                Object aggBuffer = aggFunctions[i].getAggBuffer();
                aggregationBuffers[i] = aggBuffer;
            }
        }

        updateAggregations(aggregationBuffers, row, objParser);
    }

    private void updateAggregations(Object[] aggregationBuf, Object row, ObjectParser objParser) {
        for (int i = 0; i < aggregationBuf.length; i++) {
            Object[] objects = new Object[aggParameterFields.length];
            for (int j = 0; j < aggParameterFields[i].length; j++) {
                objects[j] = aggParameterFields[i][j].eval(row);
            }
            aggFunctions[i].aggregate(aggregationBuf[i], objects);
        }
    }
}
