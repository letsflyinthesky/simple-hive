lexer grammar SimpleLexer;


@header {
package com.example.parse;
}

// Keywords

KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_ALL : 'ALL';
KW_NONE: 'NONE';
KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT' | '!';
KW_LIKE : 'LIKE';
KW_ANY : 'ANY';
KW_ORDER : 'ORDER';
KW_GROUP : 'GROUP';
KW_HAVING : 'HAVING';
KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_AS : 'AS';
KW_JOIN : 'JOIN';
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_ON : 'ON';
KW_WITH: 'WITH';
LPAREN : '(' ;
RPAREN : ')' ;
KW_SELECT : 'SELECT';
KW_DISTINCT : 'DISTINCT';

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

EQUAL : '=' | '==';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

KW_BY : 'BY';
KW_INT: 'INT' | 'INTEGER';
KW_BIGINT: 'BIGINT';
KW_CASE: 'CASE';
KW_WHEN: 'WHEN';
KW_THEN: 'THEN';
KW_ELSE: 'ELSE';
KW_END: 'END';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : 'DIV';

KW_CAST: 'CAST';
KW_INNER: 'INNER';
KW_IS: 'IS';
KW_STRING: 'STRING';
KW_BOOLEAN: 'BOOLEAN';
KW_EXISTS : 'EXISTS';
KW_OUTER : 'OUTER';
KW_GROUPING: 'GROUPING';
KW_SETS: 'SETS';


/*
这里应该放在前面
*/
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

Digit
    :'0'..'9'
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* )?
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )+
    ;




WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;