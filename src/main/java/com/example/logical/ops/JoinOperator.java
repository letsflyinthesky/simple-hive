package com.example.logical.ops;

import com.example.logical.JoinCond;
import com.example.logical.expressions.ExprNode;

import java.util.*;

/**
 * @author zhishui
 */
public class JoinOperator extends Operator {

    private Map<Integer, List<ExprNode>> filterMap;
    private ExprNode[][] joinKeys;
    private List<List<Object>>[] storage;
    private int childNum;
    private boolean[][] skipVectors;
    private List<Object>[] intermediate;
    private List<Object>[] dummyObj;
    private JoinCond[] cond;
    private Object[] forwardCache;
    protected int[] offsets;

    public JoinOperator(Map<Integer, List<ExprNode>> filterMap, ExprNode[][] joinKeys) {
        this.filterMap = filterMap;
        this.joinKeys = joinKeys;
        dummyObj = new ArrayList[childNum];
        for (int i = 0; i < childNum; i++) {
            int length = joinKeys[i].length;
            List<Object> nr = new ArrayList<>(length);
            for (int j = 0; j < nr.size(); j++) {
                nr.add(null);
            }
            dummyObj[i] = nr;
        }

    }

    public ExprNode[][] getJoinKeys() {
        return joinKeys;
    }

    public void setJoinKeys(ExprNode[][] joinKeys) {
        this.joinKeys = joinKeys;
    }

    public Map<Integer, List<ExprNode>> getFilterMap() {
        return filterMap;
    }

    public void setFilterMap(Map<Integer, List<ExprNode>> filterMap) {
        this.filterMap = filterMap;
    }

    @Override
    public void process(Object row, int tag) {
        int size = storage[tag].size();

        if (tag == childNum - 1) {
            genObject();
            storage[tag].clear();
        }
//        convert
        List<Object> rowList = new ArrayList<>();
        storage[tag].add(rowList);

    }

    private void genObject() {
        Iterator<List<Object>> iterator = storage[0].iterator();
        boolean rightFirst = true;
        while (iterator.hasNext()) {
            List<Object> right = iterator.next();
            boolean rightNull = right == dummyObj[0];
            skipVectors[0][0] = rightNull;
            intermediate[0] = right;
            genObject(1, rightFirst, rightNull);
            rightFirst = false;
        }

    }

    private void genObject(int aliasNum, boolean rightFirst, boolean leftNull) {
        JoinCond joinCond = cond[aliasNum - 1];
        int type = joinCond.getType();
        int left = joinCond.getLeft();
        int right = joinCond.getRight();

        boolean[] skipVector = skipVectors[aliasNum];
        boolean[] prevSkip = skipVectors[aliasNum - 1];
        List<List<Object>> objects = storage[aliasNum];
        Iterator<List<Object>> iterator = objects.iterator();
        while (iterator.hasNext()) {
            List<Object> rightObj = iterator.next();
            boolean rightNull = rightObj == dummyObj[aliasNum];
            skipVector[aliasNum] = rightNull;
            switch (type) {
                // INNER JOIN
                case 0:
                    if (skipVector[right] || skipVector[left]) {
                        Arrays.fill(skipVector, true);
                    }
                    break;
                // LEFT JOIN
                case 1:
                    break;
                default:
                    break;
            }

            intermediate[aliasNum] = rightObj;

            if (aliasNum == childNum - 1) {
                if (!(leftNull && rightNull)) {
                    forwardJoinObject(skipVector);
                }
            }

        }


    }

    private void forwardJoinObject(boolean[] skipVector) {
        Arrays.fill(forwardCache, null);
        boolean forward = false;
        for (int i = 0; i < childNum; i++) {
            if (!skipVector[i]) {
                for (int j = offsets[i]; j < offsets[i + 1]; j++) {
                    forwardCache[j] = intermediate[i].get(j - offsets[i]);
                }
                forward = true;
            }
        }
        if (forward) {
            forward(forwardCache);
        }


    }
}
