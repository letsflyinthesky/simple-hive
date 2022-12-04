package com.example.logical;

import com.example.Context;
import com.example.logical.aggregate.AggregationDesc;
import com.example.logical.aggregate.GroupByMode;
import com.example.logical.evaluator.GenericEvaluator;
import com.example.logical.expressions.ColumnExprNode;
import com.example.logical.expressions.ConstantExprNode;
import com.example.logical.expressions.ExprNode;
import com.example.logical.functions.AggFunction;
import com.example.logical.functions.FunctionRegistry;
import com.example.logical.functions.GenericFunctionInfo;
import com.example.logical.objectparser.IntegerObjectParser;
import com.example.logical.objectparser.LongObjectParser;
import com.example.logical.objectparser.ObjectParser;
import com.example.logical.objectparser.StringObjectParser;
import com.example.logical.ops.*;
import com.example.meta.MetaStore;
import com.example.parse.ASTNode;
import com.example.parse.SimpleParser;
import com.example.physical.MapRedTask;
import com.example.physical.TaskCompiler;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * @author zhishui
 */

public class SimpleAnalyzer {

    // top query block
    private QueryBlock qb;
    private Map<String, CTEClause> aliasToCTEs;
    private Map<String, SimpleTable> tableNameToTable;
    private MetaStore metaStore;
    private LinkedHashMap<Operator, OpParseContext> opParseContext;
    private List<Operator> tableScanOps;
    private List<MapRedTask> rootTasks;

    public SimpleAnalyzer() {
        tableNameToTable = new HashMap<>();
        opParseContext = new LinkedHashMap<>();
        tableScanOps = new ArrayList<>();
        metaStore = new MetaStore();

    }

    public void analyze(ASTNode tree, Context ctx) throws SemanticException {
        init();
        bindProps(tree, qb);
        bindMetaData(qb);
        genLogicalPlan(qb);
        rootTasks = new ArrayList<>();
        genPhysicalPlan(rootTasks);
    }

    public List<MapRedTask> getRootTasks() {
        return rootTasks;
    }

    private List<MapRedTask> genPhysicalPlan(List<MapRedTask> rootTasks) throws SemanticException {
        TaskCompiler taskCompiler = new TaskCompiler();
        taskCompiler.compile(rootTasks, tableScanOps);
        return rootTasks;
    }

    private Operator genLogicalPlan(QueryBlock qb) throws SemanticException {

        Map<String, Operator> aliasToOperators = new LinkedHashMap<String, Operator>();

        for (Map.Entry<String, String> entry : qb.getAliasToTabs().entrySet()) {
            String alias = entry.getKey();
            Operator tableScanOperator = genTablePlan(alias, qb);
            aliasToOperators.put(alias, tableScanOperator);
        }

        Operator srcOperator = null;
        if (qb.getQbParseInfo().getJoinExpr() != null) {
            ASTNode joinExpr = qb.getQbParseInfo().getJoinExpr();
            QueryBlockJoinTree joinTree = genJoinTree(qb, joinExpr, aliasToOperators);
            qb.setQbJoinTree(joinTree);

            srcOperator = genJoinPlan(qb, aliasToOperators);
        } else {
            // first value
            srcOperator = aliasToOperators.values().iterator().next();
        }

        QueryBlockParseInfo qbParseInfo = qb.getQbParseInfo();
//       filter
        if (qbParseInfo.getWhereExpr() != null) {
            srcOperator = genFilterPlan(qbParseInfo.getWhereExpr(), srcOperator);
        }

//        aggregation
        if (!qbParseInfo.getAggregationExprs().isEmpty()
                || qbParseInfo.getGroupBy() != null) {
//            get group by
            ASTNode groupBy = qbParseInfo.getGroupBy();
            List<ASTNode> groupBys = new ArrayList<ASTNode>();
            boolean existGroupingSets = false;
            Map<String, Integer> exprPos = new HashMap<>();
            if (groupBy != null) {
                for (int i = 0; i < groupBy.getChildCount(); i++) {
                    ASTNode child = (ASTNode) groupBy.getChild(i);
                    if (child.getType() != SimpleParser.TOK_GROUPING_SETS_EXPRESSION) {
                        groupBys.add(child);
                        exprPos.put(child.toStringTree(), i);
                    } else if (child.getType() != SimpleParser.TOK_GROUPING_SETS_EXPRESSION) {
                        existGroupingSets = true;
                    }

                }
            }

//            check grouping sets
            List<BitSet> groupingSets = new ArrayList<>();
            if (existGroupingSets) {
                for (int i = 0; i < groupBy.getChildCount(); i++) {
                    ASTNode child = (ASTNode) groupBy.getChild(i);
                    if (child.getType() != SimpleParser.TOK_GROUPING_SETS_EXPRESSION) {
                        continue;
                    }
                    BitSet round = new BitSet();
                    for (int j = 0; j < child.getChildCount(); j++) {
                        String treeStr = child.getChild(j).toStringTree();
                        Integer integer = exprPos.get(treeStr);
                        if (integer == null) {
                            throw new SemanticException("there are not found field in grouping sets which does not exist in group by");
                        }
                        if (round.isEmpty()) {
                            round.set(integer);
                        } else {
                            BitSet bitSet = new BitSet();
                            bitSet.set(integer);
                            round.or(bitSet);
                        }
                    }
                    groupingSets.add(round);
                }
            }
            Map<String, AggFunction> aggFunctions = new LinkedHashMap<>();
            Operator aggregateOperator = genAggregateOperator(qb, groupBys, groupingSets, GroupByMode.HASH, aggFunctions, srcOperator);
            Operator reduceSinkOperator = genReduceSinkOperator(qb, groupBys, aggregateOperator);
            srcOperator = genMergeAggregateOperator(qb, groupBys, reduceSinkOperator, groupingSets, GroupByMode.MERGEPARTIAL, aggFunctions);
        }

//        select
        if (qbParseInfo.getSelectExpr() != null) {
            ASTNode selectExpr = qbParseInfo.getSelectExpr();
            srcOperator = genSelectPlan(selectExpr, qb, srcOperator);
        }
        return srcOperator;
    }

    private Operator genSelectPlan(ASTNode selectExpr, QueryBlock qb, Operator srcOperator) throws SemanticException {
        RowResolver inputRowResolver = opParseContext.get(srcOperator).getRowResolver();
        RowResolver rowResolver = new RowResolver();
        List<ExprNode> projectNodes = new ArrayList<>();
        for (int i = 0; i < selectExpr.getChildCount(); i++) {
            ASTNode child = (ASTNode) selectExpr.getChild(i);
            ExprNode exprNode = null;
            if ((child.getToken().getType() == SimpleParser.TOK_SELEXPR)
                    && (child.getChildCount() == 1)
                    && child.getChild(0).getType() == SimpleParser.TOK_TABLE_COL) {
                ASTNode columnNode = (ASTNode) child.getChild(0);
                exprNode = genExprNode(columnNode, inputRowResolver);
            } else {
                exprNode = genExprNode(child, inputRowResolver);
            }

            String internalName = genColumnInternalName(i);
            ColumnInfo columnInfo = new ColumnInfo("", internalName, exprNode.getObjectParser().getTypeName());
            rowResolver.getColumnInfos().add(columnInfo);
            projectNodes.add(exprNode);
        }

        return putIntoOpParseContext(makeChildAndGet(new SelectOperator(
                projectNodes
        ), Arrays.asList(srcOperator)), rowResolver);

    }

    //    here we complete merge partial aggregation logic
    private Operator genMergeAggregateOperator(QueryBlock qb, List<ASTNode> groupBys,
                                               Operator reduceSinkOperator,
                                               List<BitSet> groupingSets, GroupByMode mode,
                                               Map<String, AggFunction> evaluators) throws SemanticException {
        RowResolver inputRowResolver = opParseContext.get(reduceSinkOperator).getRowResolver();
        RowResolver rowResolver = new RowResolver();
        rowResolver.setAggResolver(true);
        List<ExprNode> groupByKeys = new ArrayList<>();
        for (int i = 0; i < groupBys.size(); i++) {
            ASTNode astNode = groupBys.get(i);
            ColumnInfo columnInfo = inputRowResolver.getExpression(astNode);
            if (columnInfo == null) {
                throw new SemanticException("there found no corresponding fields");
            }
            groupByKeys.add(new ColumnExprNode(columnInfo));
            String internalName = genColumnInternalName(i);
            rowResolver.putExpression(astNode, new ColumnInfo(
                    "", internalName, columnInfo.getTypeName()
            ));
        }

        LinkedHashMap<String, ASTNode> aggregationExprs = qb.getQbParseInfo().getAggregationExprs();
        List<AggregationDesc> aggregations = new ArrayList<>();
        for (Map.Entry<String, ASTNode> entry : aggregationExprs.entrySet()) {
            // does not support count(distinct )
            List<ExprNode> aggParameters = new ArrayList<>();
            ASTNode value = entry.getValue();
            ColumnInfo columnInfo = inputRowResolver.getExpression(value);
            String internalName = columnInfo.getInternalName();
            aggParameters.add(new ColumnExprNode(new ColumnInfo(columnInfo.getTabAlias(),
                    internalName, columnInfo.getTypeName())));
//            GenericFunctionInfo genericInfo = genGenericInfo(genericEvaluator, mode, aggParameters);
            String aggName = value.getChild(0).getText();
            AggFunction aggFunction = evaluators.get(entry.getKey());
            AggFunction newAggFunction = aggFunction.copy();
            AggregationDesc aggregationDesc = new AggregationDesc(aggName, aggParameters, newAggFunction, mode);
            aggFunction.setMode(GroupByMode.FINAL);
            aggregationDesc.setAggFunction(aggFunction);

            aggregations.add(new AggregationDesc(aggName, aggParameters, aggFunction, mode));
            String colName = genColumnInternalName(groupByKeys.size() + aggregations.size() - 1);
            rowResolver.putExpression(value, new ColumnInfo(
                    "", colName, aggFunction.getReturnType()
            ));
        }

        return putIntoOpParseContext(makeChildAndGet(new AggregateOperator(
                aggregations, groupByKeys, groupingSets
        ), Arrays.asList(reduceSinkOperator)), rowResolver);

    }

    private ObjectParser genObj(String typeName) {
        if (typeName.equalsIgnoreCase("INTEGER")) {
            return new IntegerObjectParser();
        } else if (typeName.equalsIgnoreCase("LONG")) {
            return new LongObjectParser();
        }else if (typeName.equalsIgnoreCase("STRING")) {
            return new StringObjectParser();
        }
        return null;
    }

    private Operator genReduceSinkOperator(QueryBlock qb, List<ASTNode> groupBys, Operator aggregateOperator) throws SemanticException {
        RowResolver inputRowResolver = this.opParseContext.get(aggregateOperator).getRowResolver();
        RowResolver rowResolver = new RowResolver();
        rowResolver.setAggResolver(true);
        List<ExprNode> reduceKeys = new ArrayList<>();
        int aggPos = 0;
        // generate sort keys
        List<ObjectParser> keyObjectParsers = new ArrayList<>();
        for (int i = 0; i < groupBys.size(); i++) {
            ASTNode groupByNode = groupBys.get(i);
            ExprNode exprNode = genExprNode(groupByNode, inputRowResolver);
            reduceKeys.add(exprNode);
            String internalName = genColumnInternalName(i);
            rowResolver.putExpression(groupByNode, new ColumnInfo(
                    null, internalName, exprNode.getObjectParser().getTypeName()));
            aggPos++;

        }

//      TODO  add grouping sets id

        LinkedHashMap<String, ASTNode> aggregationExprs = qb.getQbParseInfo().getAggregationExprs();
        List<ExprNode> values = new ArrayList<>();
        for (Map.Entry<String, ASTNode> entry : aggregationExprs.entrySet()) {
            ColumnInfo columnInfo = inputRowResolver.getColumnInfos().get(aggPos);
//            from aggNode to column Node
            String internalName = genColumnInternalName(aggPos);
            ColumnExprNode exprNode = new ColumnExprNode(new ColumnInfo("", internalName, columnInfo.getTypeName()));
            values.add(exprNode);
            aggPos++;
            rowResolver.putExpression(entry.getValue(),
                    new ColumnInfo("", internalName, columnInfo.getTypeName()));
        }

        return putIntoOpParseContext(makeChildAndGet(new ReduceSinkOperator(
                reduceKeys, values
        ), Arrays.asList(aggregateOperator)), rowResolver);

    }

    private Operator genAggregateOperator(QueryBlock qb, List<ASTNode> groupBys, List<BitSet> groupingSets, GroupByMode mode,
                                          Map<String, AggFunction> aggFunctions, Operator input) throws SemanticException {

        RowResolver rowResolver = new RowResolver();
        rowResolver.setAggResolver(true);
        RowResolver inputResolver = this.opParseContext.get(input).getRowResolver();
        List<ExprNode> groupByNodes = new ArrayList<>();
        List<AggregationDesc> aggregations = new ArrayList<>();

        for (int i = 0; i < groupBys.size(); i++) {
            ASTNode groupByNode = groupBys.get(i);
            ExprNode exprNode = genExprNode(groupByNode, inputResolver);
            groupByNodes.add(exprNode);
            String internalName = genColumnInternalName(i);
            rowResolver.putExpression(groupByNode, new ColumnInfo(
                    "", internalName, exprNode.getObjectParser().getTypeName()
            ));
        }

        LinkedHashMap<String, ASTNode> aggregationExprs = qb.getQbParseInfo().getAggregationExprs();
        for (Map.Entry<String, ASTNode> entry : aggregationExprs.entrySet()) {
            ASTNode value = entry.getValue();
            String aggName = value.getChild(0).getText();
            List<ExprNode> aggParameters = new ArrayList<>();
            // 0 is aggregation function
            for (int i = 1; i < value.getChildCount(); i++) {
                ASTNode child = (ASTNode) value.getChild(i);
                ExprNode exprNode = genExprNode(child, inputResolver);
                aggParameters.add(exprNode);
            }
            String internalName = genColumnInternalName(groupByNodes.size() + aggregations.size() - 1);
            // here we simplify
            AggFunction aggFunction = FunctionRegistry.getAggFunctionWithNodes(aggName, aggParameters, mode);
            AggregationDesc aggregationDesc = new AggregationDesc(aggName, aggParameters,
                    aggFunction, mode);

            aggregations.add(aggregationDesc);
            rowResolver.putExpression(value, new ColumnInfo("", internalName,
                    aggFunction.getReturnType()));
            aggFunctions.put(entry.getKey(), aggFunction);
        }

        return putIntoOpParseContext(makeChildAndGet(new AggregateOperator(
                aggregations, groupByNodes, groupingSets
        ), Arrays.asList(input)), rowResolver);
    }

    private GenericFunctionInfo genGenericInfo(AggFunction genericEvaluator, GroupByMode mode, List<ExprNode> aggParameters) {
        return null;
    }

//    private GenericEvaluator getGenericEvaluator(String aggName, List<ExprNode> aggParameters) throws SemanticException {
//        List<ObjectParser> objOps = new ArrayList<>();
//        for (ExprNode aggNode : aggParameters) {
//            objOps.add(aggNode.getObjectParser());
//        }
//        GenericEvaluator genericEvaluator = FunctionRegistry.getAggFunction(aggName, objOps);
//        if (genericEvaluator == null) {
//            throw new SemanticException(String.format("there does not exist function %s", aggName));
//        }
//
//        return genericEvaluator;
//    }

    public String genColumnInternalName(int i) {
        return String.format("_col%d", i);
    }

    private Operator genFilterPlan(ASTNode whereExpr, Operator input) throws SemanticException {
        OpParseContext opParseContext = this.opParseContext.get(input);
        RowResolver rowResolver = opParseContext.getRowResolver();
        ExprNode exprNode = genExprNode(whereExpr, rowResolver);

        if (exprNode instanceof ConstantExprNode) {
            Object value = ((ConstantExprNode) exprNode).getValue();
            if (Boolean.TRUE.equals(value)) {
                return input;
            }
        }
        return putIntoOpParseContext(makeChildAndGet(new FilterOperator(exprNode), Arrays.asList(input)), rowResolver);
    }

    //    here we are different from hive logical
    private Operator makeChildAndGet(Operator parent, List<Operator> childList) {
        if (childList == null || childList.size() == 0) {
            return parent;
        }

        List<Operator> children = parent.getChildren();
        for (Operator op : childList) {
            List<Operator> parents = op.getParents();
            parents.add(parent);
            children.add(op);
        }
        return parent;
    }

    private Operator putIntoOpParseContext(Operator op, RowResolver rowResolver) {
        OpParseContext context = new OpParseContext();
        context.setRowResolver(rowResolver);
        opParseContext.put(op, context);
        return op;
    }

    //    here we care about constant/function/case when
//    no subquery
    private ExprNode genExprNode(ASTNode node, RowResolver inputResolver) throws SemanticException {
        return ExpressionConversion.genExprNode(node, inputResolver);
    }

    private Operator genJoinPlan(QueryBlock qb, Map<String, Operator> aliasToOperators) throws SemanticException {
        QueryBlockJoinTree qbJoinTree = qb.getQbJoinTree();
        return genJoinOperator(qb, qbJoinTree, null, aliasToOperators);
    }

    //支持两个以上JOIN
    private Operator genJoinOperator(QueryBlock qb, QueryBlockJoinTree qbJoinTree, Operator inputOperator,
                                     Map<String, Operator> aliasToOperators) throws SemanticException {
        QueryBlockJoinTree joinSrc = qbJoinTree.getJoinSrc();
        RowResolver rowResolver = new RowResolver();
        Operator joinSrcOp = inputOperator instanceof JoinOperator ? inputOperator : null;
        if (inputOperator == null && joinSrc != null) {
            genJoinOperator(qb, joinSrc, null, aliasToOperators);
        }

        String[] baseSrc = qbJoinTree.getBaseSrc();
        Operator[] srcOps = new Operator[baseSrc.length];
        int i = 0;
        for (String src : baseSrc) {
            if (src != null) {
                Operator operator = aliasToOperators.get(src);
                srcOps[i++] = operator;
            } else {
                srcOps[i++] = joinSrcOp;
            }
        }

        ExprNode[][] joinKeys = genJoinKeys(qbJoinTree, srcOps);
        for (int j = 0; j < srcOps.length; j++) {
            srcOps[j] = genJoinReduceSinkOperator(qb, joinKeys[j], srcOps[j]);
        }

        Map<Integer, List<ExprNode>> filterMap = new HashMap<>();
        for (int k = 0; k < srcOps.length; k++) {
            Operator input = srcOps[k] == null ? joinSrcOp : srcOps[k];
            ReduceSinkOperator rsInput = (ReduceSinkOperator) input;
            Operator operator = rsInput.getChildren().get(0);
            RowResolver inputRowResolver = opParseContext.get(operator).getRowResolver();
            List<ExprNode> reduceKeys = rsInput.getReduceKeys();
            int exprPos = 0;
            for (ExprNode expr : reduceKeys) {
                ColumnExprNode inputExpr = (ColumnExprNode) expr;
                ColumnInfo inputCI = inputExpr.getColumnInfo();
                ColumnInfo columnInfo = new ColumnInfo(inputCI.getTabAlias(), inputCI.getAlias(), inputCI.getTypeName());
                String internalName = genColumnInternalName(exprPos);
                columnInfo.setInternalName(internalName);
                ColumnExprNode columnExprNode = new ColumnExprNode(columnInfo);
                exprPos++;
                rowResolver.put(inputCI.getTabAlias(), inputCI.getAlias(), columnInfo);
            }
            int tag = rsInput.getTag();
            List<ExprNode> filters = new ArrayList<>();
            for (ASTNode astNode : qbJoinTree.getFilters().get(tag)) {
                filters.add(genExprNode(astNode, inputRowResolver));
            }
            filterMap.put(tag, filters);
        }

        return putIntoOpParseContext(makeChildAndGet(new JoinOperator(
                filterMap, joinKeys
        ), Arrays.asList(srcOps)), rowResolver);


    }

    private Operator genJoinReduceSinkOperator(QueryBlock qb, ExprNode[] joinKeys, Operator child) {
        RowResolver inputRowResolver = opParseContext.get(child).getRowResolver();
        RowResolver rowResolver = new RowResolver();
        List<ExprNode> reduceKeys = new ArrayList<>();
        List<ExprNode> reduceValues = new ArrayList<>();
        for (ExprNode joinKey : joinKeys) {
            reduceKeys.add(joinKey);
        }

        List<ColumnInfo> columnInfos = inputRowResolver.getColumnInfos();
        int keySize = 0;
        int valueSize = 0;
        for (int i = 0; i < columnInfos.size(); i++) {
            ColumnInfo columnInfo = columnInfos.get(i);
            String[] names = inputRowResolver.reverseLookup(columnInfo.getInternalName());
            ColumnExprNode exprNode = new ColumnExprNode(columnInfo);

            boolean found = false;
            for (ExprNode expr : reduceKeys) {
                if (expr.isSame(exprNode)) {
                    ColumnInfo newColumnInfo = new ColumnInfo(columnInfo.getTabAlias(), columnInfo.getAlias(),
                            columnInfo.getTypeName());
                    found = true;
                    newColumnInfo.setInternalName("key" + keySize);
                    keySize++;
                    rowResolver.put(columnInfo.getTabAlias(), columnInfo.getAlias(), newColumnInfo);
                }
            }

            // values
            if (!found) {
                ColumnInfo newColumnInfo = new ColumnInfo(columnInfo.getTabAlias(), columnInfo.getAlias(),
                        columnInfo.getTypeName());
                newColumnInfo.setInternalName("value" + valueSize);
                valueSize++;
                reduceValues.add(new ColumnExprNode(newColumnInfo));
                rowResolver.put(columnInfo.getTabAlias(), columnInfo.getAlias(), newColumnInfo);
            }
        }

        return putIntoOpParseContext(makeChildAndGet(new ReduceSinkOperator(
                reduceKeys, reduceValues
        ), Arrays.asList(child)), rowResolver);

    }

    private ExprNode[][] genJoinKeys(QueryBlockJoinTree qbJoinTree, Operator[] srcOps) throws SemanticException {
        ExprNode[][] joinKeys = new ExprNode[srcOps.length][];
        for (int i = 0; i < srcOps.length; i++) {
            RowResolver rowResolver = opParseContext.get(srcOps[i]).getRowResolver();
            List<ASTNode> astNodes = qbJoinTree.getExpressions().get(i);
            joinKeys[i] = new ExprNode[astNodes.size()];
            for (int j = 0; j < astNodes.size(); j++) {
                joinKeys[i][j] = genExprNode(astNodes.get(j), rowResolver);
            }
        }
        return joinKeys;
    }

    private QueryBlockJoinTree genJoinTree(QueryBlock qb, ASTNode joinExpr, Map<String, Operator> aliasToOperators) throws SemanticException {
        QueryBlockJoinTree joinTree = new QueryBlockJoinTree();
        int type = joinExpr.getToken().getType();
        JoinCond[] joinConds = new JoinCond[1];
        switch (type) {
            case SimpleParser.TOK_LEFTOUTERJOIN:
                joinConds[0] = new JoinCond(0, 1, 1);
                break;
            case SimpleParser.TOK_RIGHTOUTERJOIN:
                joinConds[0] = new JoinCond(0, 1, 2);
                break;
            case SimpleParser.TOK_FULLOUTERJOIN:
                joinConds[0] = new JoinCond(0, 1, 3);
                break;
            default:
                joinConds[0] = new JoinCond(0, 1, 0);
        }
        joinTree.setJoinConds(joinConds);
        ASTNode left = (ASTNode) joinExpr.getChild(0);
        ASTNode right = (ASTNode) joinExpr.getChild(1);

        if (isJoinToken(left)) {
            QueryBlockJoinTree leftJoinTree = genJoinTree(qb, left, aliasToOperators);
            joinTree.setJoinSrc(leftJoinTree);
            String[] leftChildAliases = leftJoinTree.getLeftAliases();
            String[] leftAliases = new String[leftChildAliases.length + 1];
            for (int i = 0; i < leftChildAliases.length; i++) {
                leftAliases[i] = leftChildAliases[i];
            }
            leftAliases[leftChildAliases.length] = leftJoinTree.getRightAliases()[0];
            joinTree.setLeftAliases(leftAliases);
        } else {
            String leftAlias = left.getChild(0).getText();
            String[] children = new String[2];
            children[0] = leftAlias;
            joinTree.setBaseSrc(children);
            String[] leftAliases = new String[1];
            leftAliases[0] = leftAlias;
        }

        String rightAlias = right.getChild(0).getText();
        String[] baseSrc = joinTree.getBaseSrc();
        if (baseSrc == null) {
            baseSrc = new String[2];
        }
        baseSrc[1] = rightAlias;
        String[] rightAliases = new String[1];
        rightAliases[0] = rightAlias;
        joinTree.setRightAliases(rightAliases);

        List<List<ASTNode>> expressions = new ArrayList<>();
        expressions.add(new ArrayList<>());
        expressions.add(new ArrayList<>());
        joinTree.setExpressions(expressions);
        ASTNode joinCondNode = (ASTNode) joinExpr.getChild(2);
        parseJoinCondition(joinTree, joinCondNode, aliasToOperators);
        return joinTree;
    }

    private void parseJoinCondition(QueryBlockJoinTree joinTree, ASTNode joinCond, Map<String, Operator> aliasToOpInfo) throws SemanticException {
        if (joinCond != null) {
            return;
        }

        switch (joinCond.getToken().getType()){
            case SimpleParser.KW_AND:
                parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(0), aliasToOpInfo);
                parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(1), aliasToOpInfo);
                break;
            case SimpleParser.EQUAL:
                ASTNode leftCond = (ASTNode) joinCond.getChild(0);
                List<String> leftRefs1 = new ArrayList<>();
                List<String> leftRefs2 = new ArrayList<>();
                parseJoinCond(joinTree, leftCond, leftRefs1, leftRefs2, aliasToOpInfo);
                break;
            default:
                throw new SemanticException("we don't support complex condition");
        }
    }

    private void parseJoinCond(QueryBlockJoinTree joinTree, ASTNode condNode, List<String> leftRefs1, List<String> leftRefs2, Map<String, Operator> aliasToOpInfo) throws SemanticException {
        switch (condNode.getToken().getType()) {
            case SimpleParser.TOK_TABLE_COL:
                String column = condNode.getChild(0).getText();
                String tableAlias = findAlias(column, aliasToOpInfo);
                for (String alias : joinTree.getLeftAliases()) {
                    if (alias.equals(tableAlias)) {
                        leftRefs1.add(alias);
                    }
                }
                for (String alias : joinTree.getRightAliases()) {
                    if (alias.equals(tableAlias)) {
                        leftRefs2.add(alias);
                    }
                }
                break;
            default:
                throw new SemanticException("we don't support complex condition");
        }
    }

    private String findAlias(String column, Map<String, Operator> aliasToOpInfo) throws SemanticException {
        String tableAlias = null;
        for (Map.Entry<String, Operator> entry : aliasToOpInfo.entrySet()) {
            RowResolver rowResolver = opParseContext.get(entry.getValue()).getRowResolver();
            ColumnInfo columnInfo = rowResolver.get(null, column);
            if (columnInfo != null) {
                if (tableAlias == null) {
                    tableAlias = entry.getKey();
                } else {
                    throw new SemanticException(String.format("there found multi columns named %s", column));
                }
            }
        }
        return tableAlias;
    }

    private Operator genTablePlan(String alias, QueryBlock qb) {
        RowResolver rowResolver = new RowResolver();
        SimpleTable simpleTable = qb.getQbMetaData().getAliasToTable().get(alias);
        List<String> fieldNames = simpleTable.getFieldNames();
        List<String> fieldTypes = simpleTable.getFieldTypes();
        for (int i = 0; i < fieldNames.size(); i++) {
            ColumnInfo columnInfo = new ColumnInfo(alias, fieldNames.get(i), fieldTypes.get(i));
            rowResolver.put(alias, fieldNames.get(i), columnInfo);
        }
        TableScanOperator scanOperator = new TableScanOperator(simpleTable);
        tableScanOps.add(scanOperator);
        putIntoOpParseContext(makeChildAndGet(scanOperator, Lists.newArrayList()), rowResolver);
        return scanOperator;
    }


    private void bindMetaData(QueryBlock qb) {
        HashMap<String, String> aliasToTabs = qb.getAliasToTabs();
        for (Map.Entry<String, String> entry : aliasToTabs.entrySet()) {
            String alias = entry.getKey();
            String tableName = entry.getValue();
            SimpleTable table = null;
            if (tableNameToTable.containsKey(tableName)) {
                table = tableNameToTable.get(tableName);
            } else {
                table = metaStore.getTable(tableName);
                tableNameToTable.put(tableName, table);
            }
            qb.getQbMetaData().setAliasToTable(alias, table);
        }
    }


    private void init() {
        QueryBlock qb = new QueryBlock();
        this.qb = qb;
    }


    public void bindProps(ASTNode astNode, QueryBlock qb) throws SemanticException {
        boolean skipRecursion = false;

        QueryBlockParseInfo qbParseInfo = qb.getQbParseInfo();

        if (astNode.getToken() != null) {
            skipRecursion = true;
            switch (astNode.getToken().getType()) {
                case SimpleParser.TOK_SELECT:
                    qbParseInfo.setSelectExpr(astNode);
                    qbParseInfo.setAggregationExprs(doGetAggregationsFromSelect(astNode, qb));
                    doGetColumnAliasesFromSelect(astNode, qbParseInfo);
                    break;

                case SimpleParser.TOK_WHERE:
                    qbParseInfo.setWhereExpr(astNode);
                    break;
                case SimpleParser.TOK_FROM:
                    int childCount = astNode.getChildCount();
                    if (childCount != 1) {
                        throw new SemanticException("we don't support multi table in from sub clause");
                    }

                    ASTNode from = (ASTNode) astNode.getChild(0);
                    if (from.getToken().getType() == SimpleParser.TOK_TABREF) {
                        processTable(from, qb);
                    } else if (isJoinToken(astNode)) {
                        processJoin(astNode, qb);
                        qbParseInfo.setJoinExpr(astNode);
                    }
                    break;

                case SimpleParser.TOK_GROUPBY:
                case SimpleParser.TOK_GROUPING_SETS:
                    qbParseInfo.setGroupBy(astNode);
                    skipRecursion = true;
                    break;
                case SimpleParser.TOK_CTE:
                    int cteChildCount = astNode.getChildCount();
                    for (int i = 0; i < cteChildCount; i++) {
                        ASTNode cteNode = (ASTNode) astNode.getChild(i);
                        ASTNode cteQuery = (ASTNode) cteNode.getChild(0);
                        String alias = cteNode.getChild(1).getText().trim();

                        String qbName = qb.getId() == null ? "" : qb.getId();
                        qbName += alias.toLowerCase();
                        if (aliasToCTEs.containsKey(qbName)) {
                            throw new SemanticException("there are ambiguous ctes in one level query");
                        }
                        aliasToCTEs.put(qbName, new CTEClause(qbName, cteQuery));
                    }
                    break;
                default:
                    skipRecursion = false;
            }
        }

        if (!skipRecursion) {
            for (int i = 0; i < astNode.getChildCount(); i++) {
                bindProps((ASTNode) astNode.getChild(i), qb);
            }
        }
    }

    private void processJoin(ASTNode astNode, QueryBlock qb) {
        int joinChildCount = astNode.getChildCount();
        for (int i = 0; i < joinChildCount; i++) {
            ASTNode child = (ASTNode) astNode.getChild(i);
            if (child.getToken().getType() == SimpleParser.TOK_TABREF) {
                processTable(child, qb);
            } else if (isJoinToken(child)) {
                processJoin(child, qb);
            }
        }
    }

    private void processTable(ASTNode tableNode, QueryBlock qb) {
        ASTNode tableTree = (ASTNode) tableNode.getChild(0);
        String tableName = null;
        String alias = null;
        if (tableTree.getToken().getType() == SimpleParser.TOK_TABNAME) {
            if (tableTree.getChildCount() == 2) {
                String dbName = tableTree.getChild(0).getText().trim();
                String tabName = tableTree.getChild(1).getText().trim();
                tableName = dbName + "." + tabName;
                alias = tableName;
            } else {
                String tabName = tableTree.getChild(0).getText().trim();
                tableName = tabName;
                alias = tableName;
            }
        }
        qb.setAliasToTabs(alias, tableName);
        qb.getQbParseInfo().setAlias(tableName);
        qb.getQbParseInfo().getAliasToSrc().put(tableName, tableTree);
    }


    private boolean isJoinToken(ASTNode astNode) {
        if (astNode.getToken().getType() == SimpleParser.TOK_JOIN
                || astNode.getToken().getType() == SimpleParser.TOK_LEFTOUTERJOIN
                || astNode.getToken().getType() == SimpleParser.TOK_RIGHTOUTERJOIN
                || astNode.getToken().getType() == SimpleParser.TOK_FULLOUTERJOIN
        ) {
            return true;
        }
        return false;
    }

    private void doGetColumnAliasesFromSelect(ASTNode astNode, QueryBlockParseInfo qbParseInfo) {
        for (int i = 0; i < astNode.getChildCount(); i++) {
            ASTNode child = (ASTNode) astNode.getChild(i);
            if ((child.getToken().getType() == SimpleParser.TOK_SELEXPR)
                    && (child.getChildCount() == 1)
                    && child.getChild(0).getType() == SimpleParser.TOK_TABLE_COL)  {
                String columnAlias = child.getChild(0).getText();
                qbParseInfo.setExprToColumnAlias((ASTNode) child.getChild(0), columnAlias);
            }
        }
    }


    private LinkedHashMap<String, ASTNode> doGetAggregationsFromSelect(ASTNode astNode, QueryBlock qb) throws SemanticException {
        LinkedHashMap<String, ASTNode> aggregationTrees = new LinkedHashMap<String, ASTNode>();
        for (int i = 0; i < astNode.getChildCount(); i++) {
            ASTNode child = (ASTNode) astNode.getChild(i);
            if (child.getType() == SimpleParser.TOK_SELEXPR ||
                    child.getType() == SimpleParser.TOK_SUBQUERY_EXPR) {
                child = (ASTNode) child.getChild(0);
            }
            doGetAllAggregations(child, aggregationTrees);
        }
        return aggregationTrees;
    }

    private void doGetAllAggregations(ASTNode astNode, LinkedHashMap<String, ASTNode> aggregationTrees) throws SemanticException {
        int exprTokenType = astNode.getToken().getType();
        if (exprTokenType == SimpleParser.TOK_SUBQUERY_EXPR) {
            // we will parse later
            return;
        }

        // we only parse function
        if (exprTokenType == SimpleParser.TOK_FUNCTION) {
            if (astNode.getChild(0).getType() == SimpleParser.Identifier) {
                String functionName = astNode.getChild(0).getText();
                if (FunctionRegistry.getFunctionInfo(functionName) == null) {
                    throw new SemanticException("There is no function found");
                }
                aggregationTrees.put(astNode.toStringTree(), astNode);
                return;
            }
        }

        // recurse for we support recursive function
        for (int i = 0; i < astNode.getChildCount(); i++) {
            doGetAllAggregations((ASTNode) astNode.getChild(i), aggregationTrees);
        }
    }


}

class CTEClause {
    String alias;
    ASTNode cteNode;

    public CTEClause() {
    }

    public CTEClause(String alias, ASTNode cteNode) {
        this.alias = alias;
        this.cteNode = cteNode;
    }
}
