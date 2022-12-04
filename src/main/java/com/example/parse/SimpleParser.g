parser grammar SimpleParser;

options
{
tokenVocab=SimpleLexer;
output=AST;
ASTLabelType=ASTNode;
}


tokens {
TOK_TABLE_COL;
TOK_SUBQUERY;
TOK_GROUPING_SETS;
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_FROM;
TOK_TAB;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_TRUE;
TOK_FALSE;
TOK_HAVING;
TOK_SORTBY;
TOK_STRING;
TOK_LIMIT;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_CTE;
TOK_TABREF;
TOK_TABNAME;
TOK_GROUPING_SETS_EXPRESSION;
TOK_SELEXPR;
TOK_ALLCOLREF;
TOK_SUBQUERY_EXPR;
TOK_SUBQUERY_OP;
TOK_FUNCTION;
TOK_FUNCTION;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FUNCTION;
TOK_FUNCTION;
TOK_GROUPBY;
}


@header {
package com.example.parse;
}


// rule of start
statement
    : queryStatement EOF;

queryStatement
    : (w=withClause)?
    queryStatementBody {
        if ($w.tree != null) {
            /* so that we could parse queryStatement directly
            */
            $queryStatementBody.tree.insertChild(0, $w.tree);
        }
    }
    -> queryStatementBody
    ;

withClause
    :
    KW_WITH cteStatement (COMMA cteStatement)* -> ^(TOK_CTE cteStatement+)
    ;

cteStatement
    :
    identifier KW_AS LPAREN queryStatement RPAREN
    -> ^(TOK_SUBQUERY queryStatement identifier)
    ;

queryStatementBody
    : regularQuery
    ;

/*
only support select clause currently
*/
regularQuery
    :
    selectStatement
    ;

selectStatement
    :
    defaultSelectStatement
    ;


defaultSelectStatement
    :
    s=selectClause
    f=fromClause?
    w=whereClause?
    g=groupByClause?
    -> ^(TOK_QUERY $f? $s $w? $g?)
    |
    LPAREN! selectStatement RPAREN!
    ;

fromClause
    :
    KW_FROM fromSource -> ^(TOK_FROM fromSource)
    ;

fromSource
    :
    joinSource
    ;

joinSource
    :
    joinSourcePart (joinOperator^ joinSourcePart (KW_ON! expression)?)*
    ;

joinSourcePart
    :
    tableSource
    ;

tableSource
    :
    tableName -> ^(TOK_TABREF  tableName)
    ;

tableName
    :
    db=identifier DOT tab=identifier
    -> ^(TOK_TABNAME $db $tab)
    |
    tab=identifier
    -> ^(TOK_TABNAME $tab)
    ;

joinOperator
    :
     KW_JOIN                      -> TOK_JOIN
    | KW_INNER KW_JOIN             -> TOK_JOIN
    | COMMA                        -> TOK_JOIN
    | KW_LEFT  (KW_OUTER)? KW_JOIN -> TOK_LEFTOUTERJOIN
    | KW_RIGHT (KW_OUTER)? KW_JOIN -> TOK_RIGHTOUTERJOIN
    | KW_FULL  (KW_OUTER)? KW_JOIN -> TOK_FULLOUTERJOIN
    ;

whereClause
    :
    KW_WHERE predicate -> ^(TOK_WHERE predicate)
    ;

predicate
    :
    expression
    ;

groupByClause
    :
    KW_GROUP KW_BY groupByExpression
    -> groupByExpression
    ;

groupByExpression
    :
    gexpr=standardGroupByExpression (gsexpr=KW_GROUPING KW_SETS
    LPAREN groupingSetExpression (COMMA groupingSetExpression)* RPAREN)?
    -> {gsexpr != null}? ^(TOK_GROUPING_SETS {$gexpr.tree} groupingSetExpression+)
    -> {$gexpr.tree}
    ;

standardGroupByExpression
    :
    expression (COMMA expression)+
    -> ^(TOK_GROUPBY expression+)
    ;

groupingSetExpression
    :
    KW_GROUPING KW_SETS
    LPAREN
    expression? (COMMA expression)*
    RPAREN
    -> ^(TOK_GROUPING_SETS_EXPRESSION expression*)
    ;


selectClause
    :
    KW_SELECT selectList
    -> ^(TOK_SELECT selectList)
    ;

selectList
    :
    selectItem (COMMA selectItem)* -> selectItem+
    ;

selectItem
    :
    (tableAllColumns) => tableAllColumns -> ^(TOK_SELEXPR tableAllColumns)
    |
    (expression
    ((KW_AS identifier) | (KW_AS LPAREN identifier (COMMA identifier)* RPAREN))?
        ) -> ^(TOK_SELEXPR expression identifier*)
    ;


tableAllColumns
    : STAR
        -> ^(TOK_ALLCOLREF)
    | tableName DOT STAR
        -> ^(TOK_ALLCOLREF tableName)
    ;

expression
    :
    booleanExpression
    ;

booleanExpression
    :
    orExpression
    ;

orExpression
    :
    andExpression (KW_OR^ andExpression)*
    ;

andExpression
    :
    notExpression (KW_AND^ notExpression)*
    ;

notExpression
    :
    (KW_NOT^)* equalExpression;


equalExpression
    :
    (similarExpression -> similarExpression)
    (
        equal=equalOperator p=similarExpression
        -> ^($equal {$equalExpression.tree} $p)

    )*
    -> {$equalExpression.tree}
    ;

similarExpression
    :
    similarExpressionMain
    |
    KW_EXISTS subQueryExpression -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_EXISTS) subQueryExpression)
    ;

similarExpressionMain
    :
    a=fieldExpression part=similarExpressionPart[$fieldExpression.tree]?
    -> {part == null}? {$a.tree}
    -> {$part.tree}
    ;

similarExpressionPart[CommonTree t]
    :
    (similarOperator equalExpr=fieldExpression)
    -> ^(similarOperator {$t} $equalExpr)
    ;

fieldExpression
    :
    fieldExpressionBody (DOT^ identifier)*
    ;

fieldExpressionBody
    :
    constant
    | castExpression
    | caseExpression
    | whenExpression
    | (functionName LPAREN) => function
    | tableColumn
    ;

function
    :
    functionName
    LPAREN
        (
        (STAR) => (star=STAR) | (expression (COMMA expression)*)?
        )
    RPAREN -> ^(TOK_FUNCTION functionName (expression+)?)
    ;

functionName
    :
    identifier
    ;


equalOperator
    :
    EQUAL | EQUAL_NS | NOTEQUAL | KW_IS KW_NOT KW_DISTINCT KW_FROM -> EQUAL_NS["<=>"]
    ;

similarOperator
    :
    LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;


constant
    :
    Number
    | StringLiteral
    | booleanValue
    ;


booleanValue
    :
    KW_TRUE^ | KW_FALSE^
    ;

castExpression
    :
    KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN -> ^(TOK_FUNCTION primitiveType expression)
    ;

primitiveType
    :
    KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_STRING        ->    TOK_STRING
    ;

caseExpression
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_CASE expression*)
    ;

whenExpression
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_WHEN expression*)
    ;

tableColumn
    :
    identifier -> ^(TOK_TABLE_COL identifier)
    ;

identifier
    :
    Identifier
    ;


subQueryExpression
    :
    LPAREN! selectStatement RPAREN!
    ;









































