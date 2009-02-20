grammar Hive;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=true;
}
 
tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;
TOK_LOCAL_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_COLREF;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_COLLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_UNION;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_LOAD;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_STRING;
TOK_LIST;
TOK_MAP;
TOK_CREATETABLE;
TOK_DESCTABLE;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_DROPPARTS;
TOK_SHOWTABLES;
TOK_SHOWPARTITIONS;
TOK_CREATEEXTTABLE;
TOK_DROPTABLE;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEBUCKETS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TBLSEQUENCEFILE;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_TABLESAMPLE;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_EXPLAIN;
TOK_TABLESERIALIZER;
TOK_TABLSERDEPROPERTIES;
TOK_TABLESERDEPROPLIST;
TOK_LIMIT;
TOKTABLESERDEPROPERTY;
}


// Package headers
@header {
package org.apache.hadoop.hive.ql.parse;
}
@lexer::header {package org.apache.hadoop.hive.ql.parse;}

@rulecatch {
catch (RecognitionException e) {
  reportError(e);
  throw e;
}
}
 
// starting rule
statement
	: explainStatement EOF
	| execStatement EOF
	;

explainStatement
	: KW_EXPLAIN (isExtended=KW_EXTENDED)? execStatement -> ^(TOK_EXPLAIN execStatement $isExtended?)
	;
		
execStatement
    : queryStatementExpression
    | loadStatement
    | ddlStatement
    ;

loadStatement
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tabName) 
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
    ;

ddlStatement
    : createStatement
    | dropStatement
    | alterStatement
    | descStatement
    | showStatement
    | createFunctionStatement
    ;

createStatement
    : KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE name=Identifier LPAREN columnNameTypeList RPAREN tableComment? tablePartition? tableBuckets? tableRowFormat? tableFileFormat? tableLocation?
    -> {$ext == null}? ^(TOK_CREATETABLE $name columnNameTypeList tableComment? tablePartition? tableBuckets? tableRowFormat? tableFileFormat? tableLocation?)
    ->                 ^(TOK_CREATEEXTTABLE $name columnNameTypeList tableComment? tablePartition? tableBuckets? tableRowFormat? tableFileFormat? tableLocation?)
    ;

dropStatement
    : KW_DROP KW_TABLE Identifier  -> ^(TOK_DROPTABLE Identifier)
    ;

alterStatement
    : alterStatementRename
    | alterStatementAddCol
    | alterStatementDropPartitions
    ;

alterStatementRename
    : KW_ALTER KW_TABLE oldName=Identifier KW_RENAME KW_TO newName=Identifier 
    -> ^(TOK_ALTERTABLE_RENAME $oldName $newName)
    ;

alterStatementAddCol
    : KW_ALTER KW_TABLE Identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS Identifier columnNameTypeList)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS Identifier columnNameTypeList)
    ;

alterStatementDropPartitions
    : KW_ALTER KW_TABLE Identifier KW_DROP partitionSpec (COMMA partitionSpec)*
    -> ^(TOK_ALTERTABLE_DROPPARTS Identifier partitionSpec+)
    ;

descStatement
    : KW_DESCRIBE (isExtended=KW_EXTENDED)? (tab=tabName)  -> ^(TOK_DESCTABLE $tab $isExtended?)
    ;

showStatement
    : KW_SHOW KW_TABLES showStmtIdentifier?  -> ^(TOK_SHOWTABLES showStmtIdentifier?)
    | KW_SHOW KW_PARTITIONS Identifier -> ^(TOK_SHOWPARTITIONS Identifier)
    ;
    
createFunctionStatement
    : KW_CREATE KW_TEMPORARY KW_FUNCTION Identifier KW_AS StringLiteral
    -> ^(TOK_CREATEFUNCTION Identifier StringLiteral)
    ;

showStmtIdentifier
    : Identifier
    | StringLiteral
    ;

tableComment
    :
      KW_COMMENT comment=StringLiteral  -> ^(TOK_TABLECOMMENT $comment)
    ;

tablePartition
    : KW_PARTITIONED KW_BY LPAREN columnNameTypeList RPAREN 
    -> ^(TOK_TABLEPARTCOLS columnNameTypeList)
    ;

tableBuckets
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS 
    -> ^(TOK_TABLEBUCKETS $bucketCols $sortCols? $num)
    ;

tableRowFormat
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? 
    -> ^(TOK_TABLEROWFORMAT tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?)
    | KW_ROW KW_FORMAT KW_SERIALIZER name=StringLiteral tableSerializerProperties?
    -> ^(TOK_TABLESERIALIZER $name tableSerializerProperties?)
    ;

tableSerializerProperties
    :
      KW_WITH KW_PROPERTIES LPAREN propertiesList RPAREN -> ^(TOK_TABLSERDEPROPERTIES propertiesList)
    ;

propertiesList
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_TABLESERDEPROPLIST keyValueProperty+)
    ;

keyValueProperty
    :
      key=StringLiteral EQUAL value=StringLiteral -> ^(TOKTABLESERDEPROPERTY $key $value)
    ;

tableRowFormatFieldIdentifier
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral 
    -> ^(TOK_TABLEROWFORMATFIELD $fldIdnt)
    ;

tableRowFormatCollItemsIdentifier
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATCOLLITEMS $collIdnt)
    ;

tableRowFormatMapKeysIdentifier
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATMAPKEYS $mapKeysIdnt)
    ;

tableRowFormatLinesIdentifier
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATLINES $linesIdnt)
    ;

tableFileFormat
    :
      KW_STORED KW_AS KW_SEQUENCEFILE  -> TOK_TBLSEQUENCEFILE
    ;

tableLocation
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_TABLELOCATION $locn)
    ;
  
columnNameTypeList
    : columnNameType (COMMA columnNameType)* -> ^(TOK_TABCOLLIST columnNameType+)
    ;

columnNameList
    : columnName (COMMA columnName)* -> ^(TOK_TABCOLNAME columnName+)
    ;

columnName
    :
      Identifier
    ;

columnNameOrderList
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;

columnNameOrder
    : Identifier (asc=KW_ASC | desc=KW_DESC)? 
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC Identifier)
    ->                  ^(TOK_TABSORTCOLNAMEDESC Identifier)
    ;

columnNameType
    : colName=Identifier colType (KW_COMMENT comment=StringLiteral)?    
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

colType
    : primitiveType
    | listType
    | mapType
    ;

primitiveType
    : KW_TINYINT       ->    TOK_TINYINT
    | KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_FLOAT         ->    TOK_FLOAT
    | KW_DOUBLE        ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    | KW_STRING        ->    TOK_STRING
    ;

listType
    : KW_ARRAY LESSTHAN primitiveType GREATERTHAN   -> ^(TOK_LIST primitiveType)
    ;

mapType
    : KW_MAP LESSTHAN left=primitiveType COMMA right=primitiveType GREATERTHAN
    -> ^(TOK_MAP $left $right)
    ;

queryOperator
    : KW_UNION KW_ALL -> ^(TOK_UNION)
    ;

// select statement select ... from ... where ... group by ... order by ...
queryStatementExpression
    : queryStatement (queryOperator^ queryStatementExpression)*
    ;

queryStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    | regular_body
    ;

regular_body
   :
   insertClause
   selectClause
   fromClause
   whereClause?
   groupByClause?
   orderByClause?
   clusterByClause? 
   limitClause? -> ^(TOK_QUERY fromClause ^(TOK_INSERT insertClause selectClause whereClause? groupByClause? orderByClause? clusterByClause? limitClause?))
   |
   selectClause
   fromClause
   whereClause?
   groupByClause?
   orderByClause?
   clusterByClause? 
   limitClause? -> ^(TOK_QUERY fromClause ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE)) selectClause whereClause? groupByClause? orderByClause? clusterByClause? limitClause?))
   ;


body
   :
   insertClause
   selectClause
   whereClause?
   groupByClause?
   orderByClause? 
   clusterByClause? 
   limitClause? -> ^(TOK_INSERT insertClause? selectClause whereClause? groupByClause? orderByClause? clusterByClause? limitClause?)
   |
   selectClause
   whereClause?
   groupByClause?
   orderByClause? 
   clusterByClause? 
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE)) selectClause whereClause? groupByClause? orderByClause? clusterByClause? limitClause?)
   ;
   
insertClause
   : 
   KW_INSERT KW_OVERWRITE destination -> ^(TOK_DESTINATION destination)
   ;

destination
   :
     KW_LOCAL KW_DIRECTORY StringLiteral -> ^(TOK_LOCAL_DIR StringLiteral)
   | KW_DIRECTORY StringLiteral -> ^(TOK_DIR StringLiteral)
   | KW_TABLE tabName -> ^(tabName)
   ;

limitClause
   :
   KW_LIMIT num=Number -> ^(TOK_LIMIT $num)
   ;

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
selectClause
    :
    KW_SELECT (KW_ALL | dist=KW_DISTINCT)?
    selectList -> {$dist == null}? ^(TOK_SELECT selectList)
               ->                  ^(TOK_SELECTDI selectList)
    ;

selectList
    :
    selectItem
    ( COMMA  selectItem )* -> selectItem+
    ;

selectItem
    :
      trfmClause -> ^(TOK_SELEXPR trfmClause)
    | (selectExpression  (KW_AS Identifier)?) -> ^(TOK_SELEXPR selectExpression Identifier?)
    ;
    
trfmClause
    :
    KW_TRANSFORM
    LPAREN columnList RPAREN
    KW_AS 
    LPAREN aliasList RPAREN
    KW_USING StringLiteral -> ^(TOK_TRANSFORM columnList aliasList StringLiteral)
    ;
    
selectExpression
    :
    expression | tableAllColumns
    ;

//-----------------------------------------------------------------------------------

tableAllColumns
    :
    Identifier DOT STAR -> ^(TOK_ALLCOLREF Identifier)
    ;
    
// table.column
tableColumn
    :
    (tab=Identifier)? DOT col=Identifier -> ^(TOK_COLREF $tab? $col)
    ;

columnList
    :
    tableColumn (COMMA tableColumn)* -> ^(TOK_COLLIST tableColumn+)
    ;

aliasList
    :
    Identifier (COMMA Identifier)* -> ^(TOK_ALIASLIST Identifier+)
    ;
   
//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
    :
      KW_FROM joinSource -> ^(TOK_FROM joinSource)
    | KW_FROM fromSource -> ^(TOK_FROM fromSource)
    ;

joinSource    
    :
    fromSource 
    ( joinToken^ fromSource (KW_ON! precedenceEqualExpression)? )+
    ;

joinToken
    :
      KW_JOIN                     -> TOK_JOIN
    | KW_LEFT KW_OUTER KW_JOIN    -> TOK_LEFTOUTERJOIN
    | KW_RIGHT KW_OUTER KW_JOIN   -> TOK_RIGHTOUTERJOIN
    | KW_FULL KW_OUTER KW_JOIN    -> TOK_FULLOUTERJOIN
    ;

fromSource
    :
    (tableSource | subQuerySource)
    ;
    
tableSample
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON col+=Identifier (COMMA col+=Identifier)*)? RPAREN -> ^(TOK_TABLESAMPLE $numerator $denominator $col*)
    ;

tableSource
    :
    tabname=Identifier (ts=tableSample)? (alias=Identifier)? -> ^(TOK_TABREF $tabname $ts? $alias?)
 
    ;

subQuerySource
    :
    LPAREN queryStatementExpression RPAREN Identifier -> ^(TOK_SUBQUERY queryStatementExpression Identifier)
    ;
        
//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;

searchCondition
    :
    expression
    ;

//-----------------------------------------------------------------------------------

// group by a,b
groupByClause
    :
    KW_GROUP KW_BY
    groupByExpression
    ( COMMA groupByExpression )*
    -> ^(TOK_GROUPBY groupByExpression+)
    ;

groupByExpression
    :
    expression
    ;

// order by a,b
orderByClause
    :
    KW_ORDER KW_BY
    orderByExpression
    ( COMMA orderByExpression )*
    -> ^(TOK_ORDERBY orderByExpression+)
    ;

orderByExpression
    :
    expression
    (KW_ASC | KW_DESC)?
    ;

clusterByClause
    :
    KW_CLUSTER KW_BY 
    Identifier 
    ( COMMA Identifier )* -> ^(TOK_CLUSTERBY Identifier+)
    ;

clusterByExpression
    :
    expression
    ;

// fun(par1, par2, par3)
function 
    : // LEFT and RIGHT keywords are also function names
    Identifier
    LPAREN (
          (dist=KW_DISTINCT)?
          expression
          (COMMA expression)*
        )?
    RPAREN -> {$dist == null}? ^(TOK_FUNCTION Identifier expression+)
                          -> ^(TOK_FUNCTIONDI Identifier expression+)

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
    
constant
    :
    Number
    | StringLiteral
    | charSetStringLiteral
    | booleanValue 
    ;

charSetStringLiteral
    :
    csName=CharSetName csLiteral=CharSetLiteral -> ^(TOK_CHARSETLITERAL $csName $csLiteral)
    ;

expression:
    precedenceOrExpression
    ;

atomExpression:
    KW_NULL -> TOK_NULL
    | constant
    | function
    | castExpression
    | tableColumn
    | LPAREN! expression RPAREN!
    ;


precedenceFieldExpression
    :
    atomExpression ((LSQUARE^ expression RSQUARE!) | (DOT^ Identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

precedenceUnaryExpression
    :
      precedenceFieldExpression KW_IS KW_NULL -> ^(TOK_FUNCTION TOK_ISNULL precedenceFieldExpression)
    | precedenceFieldExpression KW_IS KW_NOT KW_NULL -> ^(TOK_FUNCTION TOK_ISNOTNULL precedenceFieldExpression)
    | (precedenceUnaryOperator^)* precedenceFieldExpression
    ;


precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceUnaryExpression (precedenceBitwiseXorOperator^ precedenceUnaryExpression)*
    ;


precedenceStarOperator
    :
    STAR | DIVIDE | MOD
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator^ precedenceBitwiseXorExpression)*
    ;


precedencePlusOperator
    :
    PLUS | MINUS
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator^ precedenceStarExpression)*
    ;


precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedencePlusExpression (precedenceAmpersandOperator^ precedencePlusExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator^ precedenceAmpersandExpression)*
    ;


precedenceEqualOperator
    :
    EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    | KW_LIKE | KW_RLIKE | KW_REGEXP
    ;

precedenceEqualExpression
    :
    precedenceBitwiseOrExpression (precedenceEqualOperator^ precedenceBitwiseOrExpression)*
    ;


precedenceNotOperator
    :
    KW_NOT
    ;

precedenceNotExpression
    :
    (precedenceNotOperator^)* precedenceEqualExpression
    ;


precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator^ precedenceNotExpression)*
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator^ precedenceAndExpression)*
    ;


booleanValue
    :
    KW_TRUE^ | KW_FALSE^
    ;

tabName
   :
   Identifier partitionSpec? -> ^(TOK_TAB Identifier partitionSpec?)
   ;
        
partitionSpec
    :
    KW_PARTITION
     LPAREN partitionVal (COMMA  partitionVal )* RPAREN -> ^(TOK_PARTSPEC partitionVal +)
    ;

partitionVal
    :
    Identifier EQUAL constant -> ^(TOK_PARTVAL Identifier constant)
    ;    

// Keywords
KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_ALL : 'ALL';
KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT';
KW_LIKE : 'LIKE';

KW_ASC : 'ASC';
KW_DESC : 'DESC';
KW_ORDER : 'ORDER';
KW_BY : 'BY';
KW_GROUP : 'GROUP';
KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_AS : 'AS';
KW_SELECT : 'SELECT';
KW_DISTINCT : 'DISTINCT';
KW_INSERT : 'INSERT';
KW_OVERWRITE : 'OVERWRITE';
KW_OUTER : 'OUTER';
KW_JOIN : 'JOIN';
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_ON : 'ON';
KW_PARTITION : 'PARTITION';
KW_PARTITIONS : 'PARTITIONS';
KW_TABLE: 'TABLE';
KW_TABLES: 'TABLES';
KW_SHOW: 'SHOW';
KW_DIRECTORY: 'DIRECTORY';
KW_LOCAL: 'LOCAL';
KW_TRANSFORM : 'TRANSFORM';
KW_USING: 'USING';
KW_CLUSTER: 'CLUSTER';
KW_UNION: 'UNION';
KW_LOAD: 'LOAD';
KW_DATA: 'DATA';
KW_INPATH: 'INPATH';
KW_IS: 'IS';
KW_NULL: 'NULL';
KW_CREATE: 'CREATE';
KW_EXTERNAL: 'EXTERNAL';
KW_ALTER: 'ALTER';
KW_DESCRIBE: 'DESCRIBE';
KW_DROP: 'DROP';
KW_RENAME: 'RENAME';
KW_TO: 'TO';
KW_COMMENT: 'COMMENT';
KW_BOOLEAN: 'BOOLEAN';
KW_TINYINT: 'TINYINT';
KW_INT: 'INT';
KW_BIGINT: 'BIGINT';
KW_FLOAT: 'FLOAT';
KW_DOUBLE: 'DOUBLE';
KW_DATE: 'DATE';
KW_DATETIME: 'DATETIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_STRING: 'STRING';
KW_ARRAY: 'ARRAY';
KW_MAP: 'MAP';
KW_PARTITIONED: 'PARTITIONED';
KW_CLUSTERED: 'CLUSTERED';
KW_SORTED: 'SORTED';
KW_INTO: 'INTO';
KW_BUCKETS: 'BUCKETS';
KW_ROW: 'ROW';
KW_FORMAT: 'FORMAT';
KW_DELIMITED: 'DELIMITED';
KW_FIELDS: 'FIELDS';
KW_TERMINATED: 'TERMINATED';
KW_COLLECTION: 'COLLECTION';
KW_ITEMS: 'ITEMS';
KW_KEYS: 'KEYS';
KW_LINES: 'LINES';
KW_STORED: 'STORED';
KW_SEQUENCEFILE: 'SEQUENCEFILE';
KW_LOCATION: 'LOCATION';
KW_TABLESAMPLE: 'TABLESAMPLE';
KW_BUCKET: 'BUCKET';
KW_OUT: 'OUT';
KW_OF: 'OF';
KW_CAST: 'CAST';
KW_ADD: 'ADD';
KW_REPLACE: 'REPLACE';
KW_COLUMNS: 'COLUMNS';
KW_RLIKE: 'RLIKE';
KW_REGEXP: 'REGEXP';
KW_TEMPORARY: 'TEMPORARY';
KW_FUNCTION: 'FUNCTION';
KW_EXPLAIN: 'EXPLAIN';
KW_EXTENDED: 'EXTENDED';
KW_SERIALIZER: 'SERIALIZER';
KW_WITH: 'WITH';
KW_PROPERTIES: 'SERDEPROPERTIES';
KW_LIMIT: 'LIMIT';

// Operators

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;

EQUAL : '=';
NOTEQUAL : '<>';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
BITWISEXOR : '^';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F' 
    ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    'e' ( PLUS|MINUS )? (Digit)+
    ;

StringLiteral
    :
    '\'' (~'\'')* '\'' ( '\'' (~'\'')* '\'' )*
    ;

CharSetLiteral
    :    
    StringLiteral 
    | '0' 'X' (HexDigit|Digit)+
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    ;

CharSetName
    :
    '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
  : '--' (~('\n'|'\r'))*
    { $channel=HIDDEN; }
  ;


