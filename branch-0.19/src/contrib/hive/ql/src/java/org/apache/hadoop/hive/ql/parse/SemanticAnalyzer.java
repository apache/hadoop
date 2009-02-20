/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.util.*;
import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;

import org.antlr.runtime.tree.*;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.StringUtils;

/**
 * Implementation of the semantic analyzer
 */

public class SemanticAnalyzer extends BaseSemanticAnalyzer {
  private HashMap<String, PartitionPruner> aliasToPruner;
  private HashMap<String, SamplePruner> aliasToSamplePruner;
  private HashMap<String, Operator<? extends Serializable>> topOps;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;

  private static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(HiveConf conf) throws SemanticException {

    super(conf);

    this.aliasToPruner = new HashMap<String, PartitionPruner>();
    this.aliasToSamplePruner = new HashMap<String, SamplePruner>();
    this.topOps = new HashMap<String, Operator<? extends Serializable>>();
    this.loadTableWork = new ArrayList<loadTableDesc>();
    this.loadFileWork = new ArrayList<loadFileDesc>();
  }

  @Override
  protected void reset() {
    super.reset();
    this.aliasToPruner.clear();
    this.topOps.clear();
    this.loadTableWork.clear();
    this.loadFileWork.clear();
  }

  @SuppressWarnings("nls")
  private void doPhase1QBExpr(CommonTree ast, QBExpr qbexpr, String id,
      String alias) throws SemanticException {

    assert (ast.getToken() != null);
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_QUERY: {
      QB qb = new QB(id, alias, true);
      doPhase1(ast, qb, initPhase1Ctx());
      qbexpr.setOpcode(QBExpr.Opcode.NULLOP);
      qbexpr.setQB(qb);
    }
      break;
    case HiveParser.TOK_UNION: {
      qbexpr.setOpcode(QBExpr.Opcode.UNION);
      // query 1
      assert (ast.getChild(0) != null);
      QBExpr qbexpr1 = new QBExpr(alias + "-subquery1");
      doPhase1QBExpr((CommonTree) ast.getChild(0), qbexpr1, id + "-subquery1",
          alias + "-subquery1");
      qbexpr.setQBExpr1(qbexpr1);

      // query 2
      assert (ast.getChild(0) != null);
      QBExpr qbexpr2 = new QBExpr(alias + "-subquery2");
      doPhase1QBExpr((CommonTree) ast.getChild(1), qbexpr2, id + "-subquery2",
          alias + "-subquery2");
      qbexpr.setQBExpr2(qbexpr2);
    }
      break;
    }
  }

  private HashMap<String, CommonTree> doPhase1GetAggregationsFromSelect(
      CommonTree selExpr) {
    // Iterate over the selects search for aggregation Trees.
    // Use String as keys to eliminate duplicate trees.
    HashMap<String, CommonTree> aggregationTrees = new HashMap<String, CommonTree>();
    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      CommonTree sel = (CommonTree) selExpr.getChild(i).getChild(0);
      doPhase1GetAllAggregations(sel, aggregationTrees);
    }
    return aggregationTrees;
  }

  /**
   * DFS-scan the expressionTree to find all aggregation subtrees and put them
   * in aggregations.
   *
   * @param expressionTree
   * @param aggregations
   *          the key to the HashTable is the toStringTree() representation of
   *          the aggregation subtree.
   */
  private void doPhase1GetAllAggregations(CommonTree expressionTree,
      HashMap<String, CommonTree> aggregations) {
    if (expressionTree.getToken().getType() == HiveParser.TOK_FUNCTION
        || expressionTree.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
      assert (expressionTree.getChildCount() != 0);
      assert (expressionTree.getChild(0).getType() == HiveParser.Identifier);
      String functionName = expressionTree.getChild(0).getText();
      if (FunctionRegistry.getUDAF(functionName) != null) {
        aggregations.put(expressionTree.toStringTree(), expressionTree);
        return;
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllAggregations((CommonTree) expressionTree.getChild(i),
          aggregations);
    }
  }

  private CommonTree doPhase1GetDistinctFuncExpr(
      HashMap<String, CommonTree> aggregationTrees) throws SemanticException {
    CommonTree expr = null;
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      CommonTree value = entry.getValue();
      assert (value != null);
      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
        if (expr == null) {
          expr = value;
        } else {
          throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg(expr));
        }
      }
    }
    return expr;
  }

  private void processTable(QB qb, CommonTree tabref) throws SemanticException {
    // For each table reference get the table name
    // and the alias (if alias is not present, the table name
    // is used as an alias)
    boolean tableSamplePresent = false;
    int aliasIndex = 0;
    if (tabref.getChildCount() == 2) {
      // tablename tablesample
      // OR
      // tablename alias
      CommonTree ct = (CommonTree)tabref.getChild(1);
      if (ct.getToken().getType() == HiveParser.TOK_TABLESAMPLE) {
        tableSamplePresent = true;
      }
      else {
        aliasIndex = 1;
      }
    }
    else if (tabref.getChildCount() == 3) {
      // table name table sample alias
      aliasIndex = 2;
      tableSamplePresent = true;
    }
    CommonTree tableTree = (CommonTree)(tabref.getChild(0));
    String alias = tabref.getChild(aliasIndex).getText();
    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGOUS_TABLE_ALIAS.getMsg(tabref.getChild(aliasIndex)));
    }
    if (tableSamplePresent) {
      CommonTree sampleClause = (CommonTree)tabref.getChild(1);
      ArrayList<String> sampleCols = new ArrayList<String>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add(sampleClause.getChild(i).getText());
        }
      }
      // TODO: For now only support sampling on up to two columns
      // Need to change it to list of columns
      if (sampleCols.size() > 2) {
        throw new SemanticException(ErrorMsg.SAMPLE_RESTRICTION.getMsg(tabref.getChild(0)));
      }
      qb.getParseInfo().setTabSample(alias, new TableSample(
        sampleClause.getChild(0).getText(), 
        sampleClause.getChild(1).getText(),
        sampleCols)
      );
    }
    // Insert this map into the stats
    String table_name = tabref.getChild(0).getText();
    qb.setTabAlias(alias, table_name);

    qb.getParseInfo().setSrcForAlias(alias, tableTree);
  }

  private void processSubQuery(QB qb, CommonTree subq) throws SemanticException {

    // This is a subquery and must have an alias
    if (subq.getChildCount() != 2) {
      throw new SemanticException(ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(subq));
    }
    CommonTree subqref = (CommonTree) subq.getChild(0);
    String alias = subq.getChild(1).getText();

    // Recursively do the first phase of semantic analysis for the subquery
    QBExpr qbexpr = new QBExpr(alias);

    doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias);

    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGOUS_TABLE_ALIAS.getMsg(subq.getChild(1)));
    }
    // Insert this map into the stats
    qb.setSubqAlias(alias, qbexpr);
  }

  private boolean isJoinToken(CommonTree node)
  {
    if ((node.getToken().getType() == HiveParser.TOK_JOIN) ||
        (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN) ||
        (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN) ||
        (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN))
      return true;

    return false;
  }

  @SuppressWarnings("nls")
  private void processJoin(QB qb, CommonTree join) throws SemanticException {
    int numChildren = join.getChildCount();
    if ((numChildren != 2) && (numChildren != 3))
      throw new SemanticException("Join with multiple children");

    for (int num = 0; num < numChildren; num++) {
      CommonTree child = (CommonTree) join.getChild(num);
      if (child.getToken().getType() == HiveParser.TOK_TABREF)
        processTable(qb, child);
      else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY)
        processSubQuery(qb, child);
      else if (isJoinToken(child))
        processJoin(qb, child);
    }
  }

  @SuppressWarnings({"fallthrough", "nls"})
  private void doPhase1(CommonTree ast, QB qb, Phase1Ctx ctx_1)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();
    boolean skipRecursion = false;

    if (ast.getToken() != null) {
      skipRecursion = true;
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_SELECTDI:
        qb.countSelDi();
        // fall through
      case HiveParser.TOK_SELECT:
        qb.countSel();
        qbp.setSelExprForClause(ctx_1.dest, ast);
        HashMap<String, CommonTree> aggregations = doPhase1GetAggregationsFromSelect(ast);
        qbp.setAggregationExprsForClause(ctx_1.dest, aggregations);
        qbp.setDistinctFuncExprForClause(ctx_1.dest,
            doPhase1GetDistinctFuncExpr(aggregations));
        break;

      case HiveParser.TOK_WHERE: {
        qbp.setWhrExprForClause(ctx_1.dest, ast);
      }
        break;

      case HiveParser.TOK_DESTINATION: {
        ctx_1.dest = "insclause-" + ctx_1.nextNum;
        ctx_1.nextNum++;

        qbp.setDestForClause(ctx_1.dest, (CommonTree) ast.getChild(0));
      }
        break;

      case HiveParser.TOK_FROM: {
        int child_count = ast.getChildCount();
        if (child_count != 1)
          throw new SemanticException("Multiple Children " + child_count);

        // Check if this is a subquery
        CommonTree frm = (CommonTree) ast.getChild(0);
        if (frm.getToken().getType() == HiveParser.TOK_TABREF)
          processTable(qb, frm);
        else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY)
          processSubQuery(qb, frm);
        else if (isJoinToken(frm))
        {
          processJoin(qb, frm);
          qbp.setJoinExpr(frm);
        }
      }
        break;

      case HiveParser.TOK_CLUSTERBY: {
        // Get the clusterby aliases - these are aliased to the entries in the
        // select list
        qbp.setClusterByExprForClause(ctx_1.dest, ast);
      }
        break;

      case HiveParser.TOK_GROUPBY: {
        // Get the groupby aliases - these are aliased to the entries in the
        // select list
        if (qbp.getSelForClause(ctx_1.dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
          throw new SemanticException(ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY.getMsg(ast));
        }
        qbp.setGroupByExprForClause(ctx_1.dest, ast);
        skipRecursion = true;
      }
        break;
        
      case HiveParser.TOK_LIMIT: 
        {
          qbp.setDestLimit(ctx_1.dest, new Integer(ast.getChild(0).getText()));
        }
        break;
      default:
        skipRecursion = false;
        break;
      }
    }

    if (!skipRecursion) {
      // Iterate over the rest of the children
      int child_count = ast.getChildCount();
      for (int child_pos = 0; child_pos < child_count; ++child_pos) {

        // Recurse
        doPhase1((CommonTree) ast.getChild(child_pos), qb, ctx_1);
      }
    }
  }

  private void genPartitionPruners(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      genPartitionPruners(qbexpr.getQB());
    } else {
      genPartitionPruners(qbexpr.getQBExpr1());
      genPartitionPruners(qbexpr.getQBExpr2());
    }
  }

  @SuppressWarnings("nls")
  private void genPartitionPruners(QB qb) throws SemanticException {
    // Recursively prune subqueries
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genPartitionPruners(qbexpr);
    }

    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
      PartitionPruner pruner = new PartitionPruner(alias,
                                                   qb.getMetaData());
      // Pass each where clause to the pruner
      QBParseInfo qbp = qb.getParseInfo();
      for(String clause: qbp.getClauseNames()) {

        CommonTree whexp = (CommonTree)qbp.getWhrForClause(clause);

        if (pruner.getTable().isPartitioned() &&
            conf.getVar(HiveConf.ConfVars.HIVEPARTITIONPRUNER).equalsIgnoreCase("strict") &&
            (whexp == null || !pruner.hasPartitionPredicate((CommonTree)whexp.getChild(0)))) {
          throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE.getMsg(whexp != null ? whexp : qbp.getSelForClause(clause), 
                                                                             " for Alias " + alias + " Table " + pruner.getTable().getName()));
        }

        if (whexp != null) {
          pruner.addExpression((CommonTree)whexp.getChild(0));
        }
      }

      // Add the pruner to the list
      this.aliasToPruner.put(alias_id, pruner);
    }
  }

  private void genSamplePruners(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      genSamplePruners(qbexpr.getQB());
    } else {
      genSamplePruners(qbexpr.getQBExpr1());
      genSamplePruners(qbexpr.getQBExpr2());
    }
  }
  
  @SuppressWarnings("nls")
  private void genSamplePruners(QB qb) throws SemanticException {
    // Recursively prune subqueries
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genSamplePruners(qbexpr);
    }
    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
      QBParseInfo qbp = qb.getParseInfo();
      TableSample tableSample = qbp.getTabSample(alias_id);
      if (tableSample != null) {
        SamplePruner pruner = new SamplePruner(alias, tableSample);
        this.aliasToSamplePruner.put(alias_id, pruner);
      }
    }
  }

  private void getMetaData(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      getMetaData(qbexpr.getQB());
    } else {
      getMetaData(qbexpr.getQBExpr1());
      getMetaData(qbexpr.getQBExpr2());
    }
  }

  @SuppressWarnings("nls")
  private void getMetaData(QB qb) throws SemanticException {
    try {

      LOG.info("Get metadata for source tables");

      // Go over the tables and populate the related structures
      for (String alias : qb.getTabAliases()) {
        String tab_name = qb.getTabNameForAlias(alias);
        Table tab = null;
        try {
          tab = this.db.getTable(tab_name);
        }
        catch (InvalidTableException ite) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb.getParseInfo().getSrcForAlias(alias)));
        }

        qb.getMetaData().setSrcForAlias(alias, tab);
      }

      LOG.info("Get metadata for subqueries");
      // Go over the subqueries and getMetaData for these
      for (String alias : qb.getSubqAliases()) {
        QBExpr qbexpr = qb.getSubqForAlias(alias);
        getMetaData(qbexpr);
      }

      LOG.info("Get metadata for destination tables");
      // Go over all the destination structures and populate the related
      // metadata
      QBParseInfo qbp = qb.getParseInfo();

      for (String name : qbp.getClauseNamesForDest()) {
        CommonTree ast = qbp.getDestForClause(name);
        switch (ast.getToken().getType()) {
        case HiveParser.TOK_TAB: {
          tableSpec ts = new tableSpec(this.db, ast, true);

          if(ts.partSpec == null) {
            // This is a table
            qb.getMetaData().setDestForAlias(name, ts.tableHandle);
          } else {
            // This is a partition
            qb.getMetaData().setDestForAlias(name, ts.partHandle);
          }
          break;
        }
        case HiveParser.TOK_LOCAL_DIR:
        case HiveParser.TOK_DIR:
          {
            // This is a dfs file
            String fname = stripQuotes(ast.getChild(0).getText());
            if ((!qb.getParseInfo().getIsSubQ()) &&
                (((CommonTree)ast.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE))
            {
              fname = getTmpFileName();
              ctx.setResDir(new Path(fname));
              qb.setIsQuery(true);
            }
            qb.getMetaData().setDestForAlias(name, fname,
                                             (ast.getToken().getType() == HiveParser.TOK_DIR));
            break;
          }
        default:
          throw new SemanticException("Unknown Token Type " + ast.getToken().getType());
        }
      }
    } catch (HiveException e) {
      // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
  }

  @SuppressWarnings("nls")
  public static String getJEXLOpName(String name) {
    if (name.equalsIgnoreCase("AND")) {
      return "&&";
    }
    else if (name.equalsIgnoreCase("OR")) {
      return "||";
    }
    else if (name.equalsIgnoreCase("NOT")) {
      return "!";
    }
    else if (name.equalsIgnoreCase("=")) {
      return "==";
    }
    else if (name.equalsIgnoreCase("<>")) {
      return "!=";
    }
    else if (name.equalsIgnoreCase("NULL")) {
      return "== NULL";
    }
    else if (name.equalsIgnoreCase("NOT NULL")) {
      return "!= NULL";
    }
    else {
      return name;
    }
  }

  @SuppressWarnings("nls")
  public static String getJEXLFuncName(String name) {
    return "__udf__" + name;
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list)
      if (s.equals(elem))
        return true;

    return false;
  }

  @SuppressWarnings("nls")
  private void parseJoinCondPopulateAlias(QBJoinTree joinTree,
      CommonTree condn, Vector<String> leftAliases, Vector<String> rightAliases)
      throws SemanticException {
    // String[] allAliases = joinTree.getAllAliases();
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_COLREF:
      String tblName = condn.getChild(0).getText();
      if (isPresent(joinTree.getLeftAliases(), tblName)) {
        if (!leftAliases.contains(tblName))
          leftAliases.add(tblName);
      } else if (isPresent(joinTree.getRightAliases(), tblName)) {
        if (!rightAliases.contains(tblName))
          rightAliases.add(tblName);
      } else
        throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(condn.getChild(0)));
      break;

    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
      break;

    case HiveParser.TOK_FUNCTION:
      // check all the arguments
      for (int i = 1; i < condn.getChildCount(); i++)
        parseJoinCondPopulateAlias(joinTree, (CommonTree) condn.getChild(i),
            leftAliases, rightAliases);
      break;

    default:
      // This is an operator - so check whether it is unary or binary operator
      if (condn.getChildCount() == 1)
        parseJoinCondPopulateAlias(joinTree, (CommonTree) condn.getChild(0),
            leftAliases, rightAliases);
      else if (condn.getChildCount() == 2) {
        parseJoinCondPopulateAlias(joinTree, (CommonTree) condn.getChild(0),
            leftAliases, rightAliases);
        parseJoinCondPopulateAlias(joinTree, (CommonTree) condn.getChild(1),
            leftAliases, rightAliases);
      } else
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      break;
    }
  }

  private void populateAliases(Vector<String> leftAliases,
      Vector<String> rightAliases, CommonTree condn, QBJoinTree joinTree,
      Vector<String> leftSrc) throws SemanticException {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0))
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(condn));

    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases)
        if (!leftSrc.contains(s))
          leftSrc.add(s);
    } else
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_2.getMsg(condn));
  }

  private void parseJoinCondition(CommonTree joinParseTree,
      QBJoinTree joinTree, CommonTree joinCond, Vector<String> leftSrc)
      throws SemanticException {

    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_AND:
      parseJoinCondition(joinParseTree, joinTree, (CommonTree) joinCond
          .getChild(0), leftSrc);
      parseJoinCondition(joinParseTree, joinTree, (CommonTree) joinCond
          .getChild(1), leftSrc);
      break;

    case HiveParser.EQUAL:
      CommonTree leftCondn = (CommonTree) joinCond.getChild(0);
      Vector<String> leftAliases = new Vector<String>();
      Vector<String> rightAliases = new Vector<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftAliases, rightAliases);
      populateAliases(leftAliases, rightAliases, leftCondn, joinTree, leftSrc);

      CommonTree rightCondn = (CommonTree) joinCond.getChild(1);
      leftAliases.clear();
      rightAliases.clear();
      parseJoinCondPopulateAlias(joinTree, rightCondn, leftAliases,
          rightAliases);
      populateAliases(leftAliases, rightAliases, rightCondn, joinTree, leftSrc);
      break;

    default:
      break;
    }
  }

  
  @SuppressWarnings("nls")
  private OperatorInfo genFilterPlan(String dest, QB qb,
      OperatorInfo input) throws SemanticException {

    CommonTree whereExpr = qb.getParseInfo().getWhrForClause(dest);
    OperatorInfo output = (OperatorInfo)input.clone();
    output.setOp(
        OperatorFactory.getAndMakeChild(
            new filterDesc(genExprNodeDesc((CommonTree)whereExpr.getChild(0),
                                           qb.getParseInfo().getAlias(),
                                           input.getRowResolver())),
            new RowSchema(output.getRowResolver().getColumnInfos()),
                          input.getOp()
        )
    );
    LOG.debug("Created Filter Plan for " + qb.getId() + ":" + dest + " row schema: " + output.getRowResolver().toString());
    return output;
  }

  @SuppressWarnings("nls")
  private void genColList(String alias, CommonTree sel,
    ArrayList<exprNodeDesc> col_list, RowResolver input, Integer pos,
    RowResolver output) throws SemanticException {
    // TODO: Have to put in the support for AS clause

    // This is the tab.* case
    // In this case add all the columns to the fieldList
    // from the input schema
    for(ColumnInfo colInfo: input.getColumnInfos()) {
      String name = colInfo.getInternalName();
      String [] tmp = input.reverseLookup(name);
      exprNodeColumnDesc expr = new exprNodeColumnDesc(colInfo.getType(), name);
      col_list.add(expr);
      output.put(alias, tmp[1], new ColumnInfo(pos.toString(), colInfo.getType()));
      pos = Integer.valueOf(pos.intValue() + 1);
    }
  }

  @SuppressWarnings("nls")
  private OperatorInfo genScriptPlan(CommonTree trfm, QB qb,
      OperatorInfo input) throws SemanticException {

    OperatorInfo output = (OperatorInfo)input.clone();

    // Change the rws in this case
    CommonTree collist = (CommonTree) trfm.getChild(1);
    int ccount = collist.getChildCount();
    RowResolver out_rwsch = new RowResolver();
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < ccount; ++i) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append(((CommonTree)collist.getChild(i)).getText());
      out_rwsch.put(
        qb.getParseInfo().getAlias(),
        ((CommonTree)collist.getChild(i)).getText(),
        new ColumnInfo(((CommonTree)collist.getChild(i)).getText(),
                       String.class)  // Everything is a string right now
      );
    }

    output
        .setOp(OperatorFactory
            .getAndMakeChild(
                new scriptDesc(
                    stripQuotes(trfm.getChild(2).getText()),
                    PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.tabCode), sb.toString()),
                    PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.tabCode), "")),
                    new RowSchema(
                        out_rwsch.getColumnInfos()), input.getOp()));

    output.setRowResolver(out_rwsch);
    return output;
  }

  /**
   * This function is a wrapper of parseInfo.getGroupByForClause which automatically
   * translates SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY a,b,c.
   */
  static List<CommonTree> getGroupByForClause(QBParseInfo parseInfo, String dest) {
    if (parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
      CommonTree selectExprs = parseInfo.getSelForClause(dest);
      List<CommonTree> result = new ArrayList<CommonTree>(selectExprs == null 
          ? 0 : selectExprs.getChildCount());
      if (selectExprs != null) {
        for (int i = 0; i < selectExprs.getChildCount(); ++i) {
          // table.column AS alias
          CommonTree grpbyExpr = (CommonTree) selectExprs.getChild(i).getChild(0);
          result.add(grpbyExpr);
        }
      }
      return result;
    } else {
      CommonTree grpByExprs = parseInfo.getGroupByForClause(dest);
      List<CommonTree> result = new ArrayList<CommonTree>(grpByExprs == null 
          ? 0 : grpByExprs.getChildCount());
      if (grpByExprs != null) {
        for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
          CommonTree grpbyExpr = (CommonTree) grpByExprs.getChild(i);
          result.add(grpbyExpr);
        }
      }
      return result;
    }
  }
  
  private static String getColAlias(CommonTree selExpr, String defaultName) {
    if (selExpr.getChildCount() == 2) {
      // return zz for "xx + yy AS zz"
      return selExpr.getChild(1).getText(); 
    }

    CommonTree root = (CommonTree)selExpr.getChild(0);
    while (root.getType() == HiveParser.DOT || root.getType() == HiveParser.TOK_COLREF) {
      assert(root.getChildCount() == 2);
      root = (CommonTree) root.getChild(1);
    }
    if (root.getType() == HiveParser.Identifier) {
      // Return zz for "xx.zz" and "xx.yy.zz"
      return root.getText();
    } else {
      // Return defaultName if selExpr is not a simple xx.yy.zz 
      return defaultName;
    }
  }
  
  @SuppressWarnings("nls")
  private OperatorInfo genSelectPlan(String dest, QB qb,
      OperatorInfo input) throws SemanticException {

    CommonTree selExprList = qb.getParseInfo().getSelForClause(dest);

    ArrayList<exprNodeDesc> col_list = new ArrayList<exprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    CommonTree trfm = null;
    String alias = qb.getParseInfo().getAlias();
    Integer pos = Integer.valueOf(0);

    // Iterate over the selects
    for (int i = 0; i < selExprList.getChildCount(); ++i) {

      // list of the columns
      CommonTree selExpr = (CommonTree) selExprList.getChild(i);
      String colAlias = getColAlias(selExpr, "_C" + i);
      CommonTree sel = (CommonTree)selExpr.getChild(0);

      if (sel.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
        genColList(qb.getParseInfo().getAlias(), sel, col_list,
            input.getRowResolver(), pos, out_rwsch);
      } else if (sel.getToken().getType() == HiveParser.TOK_TRANSFORM) {
        if (i > 0) {
          throw new SemanticException(ErrorMsg.INVALID_TRANSFORM.getMsg(sel));
        }
        trfm = sel;
        CommonTree cols = (CommonTree) trfm.getChild(0);
        for (int j = 0; j < cols.getChildCount(); ++j) {
          CommonTree expr = (CommonTree) cols.getChild(j);
          if (expr.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
            genColList(alias, expr,
                       col_list, input.getRowResolver(),
                       pos, out_rwsch);
          } else {
            exprNodeDesc exp = genExprNodeDesc(expr, alias, input.getRowResolver());
            col_list.add(exp);
            if (!StringUtils.isEmpty(alias) &&
                (out_rwsch.get(alias, colAlias) != null)) {
              throw new SemanticException(ErrorMsg.AMBIGOUS_COLUMN.getMsg(expr.getChild(1)));
            }

            out_rwsch.put(alias, expr.getText(),
                          new ColumnInfo((Integer.valueOf(pos)).toString(),
                                         exp.getTypeInfo())); // Everything is a string right now
          }
        }
      } else {
        // Case when this is an expression
        exprNodeDesc exp = genExprNodeDesc(sel, qb.getParseInfo()
            .getAlias(), input.getRowResolver());
        col_list.add(exp);
        if (!StringUtils.isEmpty(alias) &&
            (out_rwsch.get(alias, colAlias) != null)) {
          throw new SemanticException(ErrorMsg.AMBIGOUS_COLUMN.getMsg(sel.getChild(1)));
        }
        // Since the as clause is lacking we just use the text representation
        // of the expression as the column name
        out_rwsch.put(alias, colAlias,
                      new ColumnInfo((Integer.valueOf(pos)).toString(),
                                     exp.getTypeInfo())); // Everything is a string right now
      }
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    for (int i=0; i<col_list.size(); i++) {
      if (col_list.get(i) instanceof exprNodeNullDesc) {
        col_list.set(i, new exprNodeConstantDesc(String.class, null));
      }
    }
    
    OperatorInfo output = (OperatorInfo) input.clone();
    output.setOp(OperatorFactory.getAndMakeChild(
        new selectDesc(col_list), new RowSchema(out_rwsch.getColumnInfos()),
        input.getOp()));

    output.setRowResolver(out_rwsch);

    if (trfm != null) {
      output = genScriptPlan(trfm, qb, output);
    }

    LOG.debug("Created Select Plan for clause: " + dest + " row schema: "
        + output.getRowResolver().toString());

    return output;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanGroupByOperator(
        QBParseInfo parseInfo, String dest, OperatorInfo reduceSinkOperatorInfo,
        groupByDesc.Mode mode)
    throws SemanticException {
    RowResolver groupByInputRowResolver = reduceSinkOperatorInfo.getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver.get("",text);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo.getInternalName()));
      String field = (Integer.valueOf(i)).toString();
      groupByOutputRowResolver.put("",grpbyExpr.toStringTree(),
                                   new ColumnInfo(field, exprInfo.getType()));
    }
    // For each aggregation
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      CommonTree value = entry.getValue();
      String aggName = value.getChild(0).getText();
      Class<? extends UDAF> aggClass = FunctionRegistry.getUDAF(aggName);
      assert (aggClass != null);
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      ArrayList<Class<?>> aggClasses = new ArrayList<Class<?>>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        String text = value.getChild(i).toStringTree();
        CommonTree paraExpr = (CommonTree)value.getChild(i);
        ColumnInfo paraExprInfo = groupByInputRowResolver.get("",text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert(paraExpression != null);
        aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), paraExprInfo.getInternalName()));
        aggClasses.add(paraExprInfo.getType().getPrimitiveClass());
      }

      if (null == FunctionRegistry.getUDAFMethod(aggName, aggClasses)) {
        String reason = "Looking for UDAF \"" + aggName + "\" with parameters " + aggClasses;
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg((CommonTree)value.getChild(0), reason));
      }
      
      aggregations.add(new aggregationDesc(aggClass, aggParameters,
          value.getToken().getType() == HiveParser.TOK_FUNCTIONDI));
      groupByOutputRowResolver.put("",value.toStringTree(),
                                   new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() -1).toString(),
                                                  String.class));  // Everything is a string right now
    }

    return new OperatorInfo(
        OperatorFactory.getAndMakeChild(new groupByDesc(mode, groupByKeys, aggregations),
                                        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                        reduceSinkOperatorInfo.getOp()),
        groupByOutputRowResolver
    );
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanGroupByOpForward(
        QBParseInfo parseInfo, String dest, OperatorInfo forwardOpInfo,
        groupByDesc.Mode mode)
    throws SemanticException {
    RowResolver inputRS  = forwardOpInfo.getRowResolver();
    RowResolver outputRS = new RowResolver();
    outputRS.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); i++) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = inputRS.get("",text);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo.getInternalName()));
      String field = (Integer.valueOf(i)).toString();
      outputRS.put("", text,
                   new ColumnInfo(field, exprInfo.getType()));
    }

    // For each aggregation
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      CommonTree value = entry.getValue();
      String aggName = value.getChild(0).getText();
      Class<? extends UDAF> aggClass = FunctionRegistry.getUDAF(aggName);
      assert (aggClass != null);
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      ArrayList<Class<?>> aggClasses = new ArrayList<Class<?>>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        String text = value.getChild(i).toStringTree();
        CommonTree paraExpr = (CommonTree)value.getChild(i);
        ColumnInfo paraExprInfo = inputRS.get("", text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert(paraExpression != null);
        aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), paraExprInfo.getInternalName()));
        aggClasses.add(paraExprInfo.getType().getPrimitiveClass());
      }

      if (null == FunctionRegistry.getUDAFMethod(aggName, aggClasses)) {
        String reason = "Looking for UDAF \"" + aggName + "\" with parameters " + aggClasses;
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg((CommonTree)value.getChild(0), reason));
      }
      
      aggregations.add(new aggregationDesc(aggClass, aggParameters,
          value.getToken().getType() == HiveParser.TOK_FUNCTIONDI));
      outputRS.put("",value.toStringTree(),
                                   new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() -1).toString(),
                                                  String.class));  // Everything is a string right now
    }

    return new OperatorInfo(
        OperatorFactory.getAndMakeChild(new groupByDesc(mode, groupByKeys, aggregations),
                                        new RowSchema(outputRS.getColumnInfos()),
                                        forwardOpInfo.getOp()),
        outputRS
    );
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanReduceSinkOperator(QBParseInfo parseInfo,
      String dest, OperatorInfo inputOperatorInfo, int numPartitionFields)
      throws SemanticException {
    RowResolver reduceSinkInputRowResolver = inputOperatorInfo.getRowResolver();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      reduceKeys.add(genExprNodeDesc(grpbyExpr, parseInfo.getAlias(),
          reduceSinkInputRowResolver));
      String text = grpbyExpr.toStringTree();
      if (reduceSinkOutputRowResolver.get("", text) == null) {
        reduceSinkOutputRowResolver.put("", text,
                                        new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                                                       String.class)); // Everything is a string right now
      } else {
        throw new SemanticException(ErrorMsg.DUPLICATE_GROUPBY_KEY.getMsg(grpbyExpr));
      }
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      CommonTree value = parseInfo.getDistinctFuncExprForClause(dest);
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        CommonTree parameter = (CommonTree) value.getChild(i);
        String text = parameter.toStringTree();
        if (reduceSinkOutputRowResolver.get("",text) == null) {
          reduceKeys.add(genExprNodeDesc(parameter, parseInfo.getAlias(), reduceSinkInputRowResolver));
          reduceSinkOutputRowResolver.put("", text,
                                          new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                                                         String.class)); // Everything is a string right now
        }
      }
    }

    // Put parameters to aggregations in reduceValues
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
        CommonTree value = entry.getValue();
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        CommonTree parameter = (CommonTree) value.getChild(i);
        String text = parameter.toStringTree();
        if (reduceSinkOutputRowResolver.get("",text) == null) {
          reduceValues.add(genExprNodeDesc(parameter, parseInfo.getAlias(), reduceSinkInputRowResolver));
          reduceSinkOutputRowResolver.put("", text,
                                          new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + Integer.valueOf(reduceValues.size() - 1).toString(),
                                                         String.class)); // Everything is a string right now
        }
      }
    }

    return new OperatorInfo(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, -1, numPartitionFields,
                                                                  -1, false),
                                        new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
                                        inputOperatorInfo.getOp()),
        reduceSinkOutputRowResolver
    );
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanReduceSinkOperator(QBParseInfo parseInfo,
     OperatorInfo input, CommonTree distinctText, TreeSet<String> ks)
     throws SemanticException {
    RowResolver inputRS = input.getRowResolver();
    RowResolver outputRS = new RowResolver();
    outputRS.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    // Spray on distinctText first
    if (distinctText != null)
    {
    	reduceKeys.add(genExprNodeDesc(distinctText, parseInfo.getAlias(), inputRS));
      String text = distinctText.toStringTree();
      assert (outputRS.get("", text) == null);
      outputRS.put("", text,
        new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                       String.class));
    }
    else {
      // dummy key
      reduceKeys.add(new exprNodeConstantDesc(0));
    }

    // copy the input row resolver
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    Iterator<String> keysIter = inputRS.getTableNames().iterator();
    while (keysIter.hasNext())
    {
      String key = keysIter.next();
      HashMap<String, ColumnInfo> map = inputRS.getFieldMap(key);
      Iterator<String> fNamesIter = map.keySet().iterator();
      while (fNamesIter.hasNext())
      {
        String field = fNamesIter.next();
        ColumnInfo valueInfo = inputRS.get(key, field);
        
        if (outputRS.get(key, field) == null) 
        {
        	reduceValues.add(new exprNodeColumnDesc(valueInfo.getType(), valueInfo.getInternalName()));
          outputRS.put(key, field, new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + Integer.valueOf(reduceValues.size() - 1).toString(), 
                                                  valueInfo.getType()));
        }
      }
    }
    
    for (String dest : ks) {
      List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);

      // send all the group by expressions
      for (int i = 0; i < grpByExprs.size(); ++i) {
        CommonTree grpbyExpr = grpByExprs.get(i);
        String text = grpbyExpr.toStringTree();
        if (outputRS.get("", text) == null) {
          exprNodeDesc grpbyExprNode = genExprNodeDesc(grpbyExpr, parseInfo.getAlias(), inputRS);
          reduceValues.add(grpbyExprNode);
          outputRS.put("", text,
                       new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + Integer.valueOf(reduceValues.size() - 1).toString(),
                                      grpbyExprNode.getTypeInfo())); 
        }
      }

      // send all the aggregation expressions
      HashMap<String, CommonTree> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
      for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
        CommonTree value = entry.getValue();
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          CommonTree parameter = (CommonTree) value.getChild(i);
          String text = parameter.toStringTree();
          if (outputRS.get("",text) == null) {
            exprNodeDesc pNode = genExprNodeDesc(parameter, parseInfo.getAlias(), inputRS);
            reduceValues.add(pNode);
            outputRS.put("", text,
                         new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + Integer.valueOf(reduceValues.size() - 1).toString(),
                                        pNode.getTypeInfo()));
          }
        }
      }
    }

    return new OperatorInfo(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, 
                                                                  -1, distinctText == null ? -1 : 1, -1, false),
                                        new RowSchema(outputRS.getColumnInfos()), input.getOp()),
        outputRS);
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanForwardOperator(QBParseInfo parseInfo, OperatorInfo input)
      throws SemanticException {
    RowResolver outputRS = input.getRowResolver();;

    Operator<? extends Serializable> forward = OperatorFactory.get(forwardDesc.class,
        new RowSchema(outputRS.getColumnInfos()));
    // set forward operator as child of each of input
    List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
    child.add(forward);
    input.getOp().setChildOperators(child);

    return new OperatorInfo(forward, outputRS);
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanReduceSinkOperator2MR(
      QBParseInfo parseInfo, String dest, OperatorInfo groupByOperatorInfo,
      int numPartitionFields) {
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    // Get group-by keys and store in reduceKeys
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      String field = (Integer.valueOf(i)).toString();
      reduceKeys.add(new exprNodeColumnDesc(TypeInfoFactory.getPrimitiveTypeInfo(String.class), field));
      reduceSinkOutputRowResolver2.put("", grpbyExpr.toStringTree(),
                                       new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + field,
                                                      String.class)); // Everything is a string right now
    }
    // Get partial aggregation results and store in reduceValues
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    int inputField = reduceKeys.size();
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      reduceValues.add(new exprNodeColumnDesc(TypeInfoFactory.getPrimitiveTypeInfo(String.class),
                                              (Integer.valueOf(inputField)).toString()));
      inputField++;
      reduceSinkOutputRowResolver2.put("", ((CommonTree)entry.getValue()).toStringTree(),
                                       new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + (Integer.valueOf(reduceValues.size()-1)).toString(),
                                                      String.class)); // Everything is a string right now
    }

    return new OperatorInfo(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, -1, 
                                                                  numPartitionFields, -1, true),
                                        new RowSchema(reduceSinkOutputRowResolver2.getColumnInfos()),
                                        groupByOperatorInfo.getOp()),
        reduceSinkOutputRowResolver2
    );
  }

  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlanGroupByOperator2MR(
      QBParseInfo parseInfo, String dest, OperatorInfo reduceSinkOperatorInfo2)
    throws SemanticException {
    RowResolver groupByInputRowResolver2 = reduceSinkOperatorInfo2.getRowResolver();
    RowResolver groupByOutputRowResolver2 = new RowResolver();
    groupByOutputRowResolver2.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver2.get("",text);
      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      String expression = exprInfo.getInternalName();
      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), expression));
      String field = (Integer.valueOf(i)).toString();
      groupByOutputRowResolver2.put("",grpbyExpr.toStringTree(),
                                    new ColumnInfo(field, exprInfo.getType()));
    }
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      CommonTree value = entry.getValue();
      String aggName = value.getChild(0).getText();
      Class<? extends UDAF> aggClass = FunctionRegistry.getUDAF(aggName);
      assert (aggClass != null);
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      String text = entry.getKey();
      ColumnInfo paraExprInfo = groupByInputRowResolver2.get("",text);
      if (paraExprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
      }

      String paraExpression = paraExprInfo.getInternalName();
      assert(paraExpression != null);
      aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), paraExpression));
      aggregations.add(new aggregationDesc(aggClass, aggParameters, false));
      groupByOutputRowResolver2.put("", value.toStringTree(),
                                    new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() - 1).toString(),
                                                   paraExprInfo.getType())); // Everything is a string right now
    }

    return new OperatorInfo(
        OperatorFactory.getAndMakeChild(new groupByDesc(groupByDesc.Mode.PARTIAL2, groupByKeys, aggregations),
                                        new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
                                        reduceSinkOperatorInfo2.getOp()),
        groupByOutputRowResolver2
    );
  }

  /**
   * Generate a Group-By plan using a single map-reduce job (3 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) SortGroupBy (keys = (KEY.0,KEY.1), aggregations =
   * (count_distinct(KEY.2), sum(VALUE.0), count(VALUE.1))) Select (final
   * selects)
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   */
  @SuppressWarnings({ "unused", "nls" })
  private OperatorInfo genGroupByPlan1MR(String dest, QB qb,
      OperatorInfo input) throws SemanticException {

    OperatorInfo inputOperatorInfo = input;
    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 1. Generate ReduceSinkOperator
    OperatorInfo reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(
        parseInfo, dest, inputOperatorInfo,
        getGroupByForClause(parseInfo, dest).size());


    // ////// 2. Generate GroupbyOperator
    OperatorInfo groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, groupByDesc.Mode.COMPLETE);

    return groupByOperatorInfo;
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs (5 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) NOTE: If DISTINCT_EXP is null, partition by rand() SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (count_distinct(KEY.2), sum(VALUE.0),
   * count(VALUE.1))) ReduceSink ( keys = (0,1), values=(2,3,4)) SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (sum(VALUE.0), sum(VALUE.1),
   * sum(VALUE.2))) Select (final selects)
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlan2MR(String dest, QB qb,
      OperatorInfo input) throws SemanticException {

    OperatorInfo inputOperatorInfo = input;
    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 1. Generate ReduceSinkOperator
    // There is a special case when we want the rows to be randomly distributed to  
    // reducers for load balancing problem.  That happens when there is no DISTINCT
    // operator.  We set the numPartitionColumns to -1 for this purpose.  This is 
    // captured by WritableComparableHiveObject.hashCode() function. 
    OperatorInfo reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(
        parseInfo, dest, inputOperatorInfo, (parseInfo
            .getDistinctFuncExprForClause(dest) == null ? -1
            : Integer.MAX_VALUE));

    // ////// 2. Generate GroupbyOperator
    OperatorInfo groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIAL1);

    // ////// 3. Generate ReduceSinkOperator2
    OperatorInfo reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo,
        getGroupByForClause(parseInfo, dest).size());

    // ////// 4. Generate GroupbyOperator2
    OperatorInfo groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(
        parseInfo, dest, reduceSinkOperatorInfo2);

    return groupByOperatorInfo2;
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs. The first map-reduce
   * job has already been constructed. Evaluate partial aggregates first,
   * followed by actual aggregates. The first map-reduce stage will be 
   * shared by all groupbys.
   */
  @SuppressWarnings("nls")
  private OperatorInfo genGroupByPlan3MR(String dest, QB qb, 
    OperatorInfo input) throws SemanticException {

    OperatorInfo inputOperatorInfo = input;
    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// Generate GroupbyOperator
    OperatorInfo groupByOperatorInfo = genGroupByPlanGroupByOpForward(parseInfo,
      dest, inputOperatorInfo, groupByDesc.Mode.PARTIAL1);

    // //////  Generate ReduceSinkOperator2
    OperatorInfo reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
      parseInfo, dest, groupByOperatorInfo,
      getGroupByForClause(parseInfo, dest).size());

    // ////// Generate GroupbyOperator2
    OperatorInfo groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(
      parseInfo, dest, reduceSinkOperatorInfo2);

    return groupByOperatorInfo2;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genConversionOps(String dest, QB qb,
      OperatorInfo input) throws SemanticException {

    Integer dest_type = qb.getMetaData().getDestTypeForAlias(dest);
    Table dest_tab = null;
    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE:
      {
        dest_tab = qb.getMetaData().getDestTableForAlias(dest);
        break;
      }
    case QBMetaData.DEST_PARTITION:
      {
        dest_tab = qb.getMetaData().getDestPartitionForAlias(dest).getTable();
        break;
      }
    default:
      {
        return input;
      }
    }

    return input;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genFileSinkPlan(String dest, QB qb,
      OperatorInfo input) throws SemanticException {

    // Generate the destination file
    String queryTmpdir = this.scratchDir + File.separator + this.randomid + '.' + this.pathid + '.' + dest ;
    this.pathid ++;

    // Next for the destination tables, fetch the information
    // create a temporary directory name and chain it to the plan
    String dest_path = null;
    tableDesc table_desc = null;

    Integer dest_type = qb.getMetaData().getDestTypeForAlias(dest);

    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE:
      {
        Table dest_tab = qb.getMetaData().getDestTableForAlias(dest);
        table_desc = Utilities.getTableDesc(dest_tab);

        dest_path = dest_tab.getPath().toString();
        // Create the work for moving the table
        this.loadTableWork.add(new loadTableDesc(queryTmpdir,
                                            table_desc,
                                            new HashMap<String, String>()));
        break;
      }
    case QBMetaData.DEST_PARTITION:
      {
        Partition dest_part = qb.getMetaData().getDestPartitionForAlias(dest);
        Table dest_tab = dest_part.getTable();
        table_desc = Utilities.getTableDesc(dest_tab);
        dest_path = dest_part.getPath()[0].toString();
        this.loadTableWork.add(new loadTableDesc(queryTmpdir, table_desc, dest_part.getSpec()));
        break;
      }
    case QBMetaData.DEST_LOCAL_FILE:
    case QBMetaData.DEST_DFS_FILE: {
        table_desc = Utilities.defaultTd;
        dest_path = qb.getMetaData().getDestFileForAlias(dest);
        String cols = new String();
        RowResolver inputRR = input.getRowResolver();
        Vector<ColumnInfo> colInfos = inputRR.getColumnInfos();
    
        boolean first = true;
        for (ColumnInfo colInfo:colInfos) {
        	String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
          if (!first)
            cols = cols.concat(",");
          
          first = false;
          if (nm[0] == null) 
          	cols = cols.concat(nm[1]);
          else
          	cols = cols.concat(nm[0] + "." + nm[1]);
        }
        
        this.loadFileWork.add(new loadFileDesc(queryTmpdir, dest_path,
                                          (dest_type.intValue() == QBMetaData.DEST_DFS_FILE), cols));
        break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + dest_type);
    }

    OperatorInfo output = (OperatorInfo)input.clone();
    output.setOp(
      OperatorFactory.getAndMakeChild(
        new fileSinkDesc(queryTmpdir, table_desc),
        new RowSchema(output.getRowResolver().getColumnInfos()), input.getOp()
      )
    );

    LOG.debug("Created FileSink Plan for clause: " + dest + "dest_path: "
        + dest_path + " row schema: "
        + output.getRowResolver().toString());
    return output;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genLimitPlan(String dest, QB qb, OperatorInfo input, int limit) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase. A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields

    OperatorInfo limitMap = (OperatorInfo)input.clone();
    limitMap.setOp(
      OperatorFactory.getAndMakeChild(
        new limitDesc(limit), new RowSchema(limitMap.getRowResolver().getColumnInfos()),
        input.getOp()
      )
    );

    LOG.debug("Created LimitOperator Plan for clause: " + dest + " row schema: "
        + limitMap.getRowResolver().toString());

    return limitMap;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genLimitMapRedPlan(String dest, QB qb, OperatorInfo input, int limit, boolean isOuterQuery) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase. A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields
    OperatorInfo curr = genLimitPlan(dest, qb, input, limit);

    if (isOuterQuery)
      return curr;

    // Create a reduceSink operator followed by another limit
    curr = genReduceSinkPlan(dest, qb, curr, 1);
    return genLimitPlan(dest, qb, curr, limit);
  }

  @SuppressWarnings("nls")
  private OperatorInfo genReduceSinkPlan(String dest, QB qb,
                                         OperatorInfo input, int numReducers) throws SemanticException {

    // First generate the expression for the key
    // The cluster by clause has the aliases for the keys
    ArrayList<exprNodeDesc> keyCols = new ArrayList<exprNodeDesc>();

    CommonTree clby = qb.getParseInfo().getClusterByForClause(dest);
    if (clby != null) {
      int ccount = clby.getChildCount();
      for(int i=0; i<ccount; ++i) {
        CommonTree cl = (CommonTree)clby.getChild(i);
        ColumnInfo colInfo = input.getRowResolver().get(qb.getParseInfo().getAlias(),
                                                        cl.getText());
        if (colInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(cl));
        }
        
        keyCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName()));
      }
    }

    ArrayList<exprNodeDesc> valueCols = new ArrayList<exprNodeDesc>();

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    for(ColumnInfo colInfo: input.getRowResolver().getColumnInfos()) {
      valueCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName()));
    }

    OperatorInfo interim = (OperatorInfo)input.clone();
    interim.setOp(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(keyCols, valueCols, -1, keyCols.size(), numReducers, false),
        new RowSchema(interim.getRowResolver().getColumnInfos()),
        input.getOp()
      )
    );

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = interim.getRowResolver();
    Integer pos = Integer.valueOf(0);
    for(ColumnInfo colInfo: interim_rwsch.getColumnInfos()) {
      String [] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1],
                    new ColumnInfo(pos.toString(), colInfo.getType()));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    OperatorInfo output = (OperatorInfo)interim.clone();
    output.setOp(
      OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(String.class, Utilities.ReduceField.VALUE.toString())),
        new RowSchema(out_rwsch.getColumnInfos()),
        interim.getOp()
      )
    );
    output.setRowResolver(out_rwsch);

    LOG.debug("Created ReduceSink Plan for clause: " + dest + " row schema: "
        + output.getRowResolver().toString());
    return output;
  }

  private OperatorInfo genJoinOperatorChildren(QBJoinTree join, OperatorInfo left,
      OperatorInfo[] right) {
    RowResolver outputRS = new RowResolver();
    // all children are base classes
    Operator<?>[] rightOps = new Operator[right.length];
    int pos = 0;
    int outputPos = 0;

    HashMap<Byte, ArrayList<exprNodeDesc>> exprMap = new HashMap<Byte, ArrayList<exprNodeDesc>>();

    for (OperatorInfo input : right)
    {
      ArrayList<exprNodeDesc> keyDesc = new ArrayList<exprNodeDesc>();
      if (input == null)
        input = left;
      Byte tag = Byte.valueOf((byte)(((reduceSinkDesc)(input.getOp().getConf())).getTag()));
      RowResolver inputRS = input.getRowResolver();
      Iterator<String> keysIter = inputRS.getTableNames().iterator();
      while (keysIter.hasNext())
      {
        String key = keysIter.next();
        HashMap<String, ColumnInfo> map = inputRS.getFieldMap(key);
        Iterator<String> fNamesIter = map.keySet().iterator();
        while (fNamesIter.hasNext())
        {
          String field = fNamesIter.next();
          ColumnInfo valueInfo = inputRS.get(key, field);
          keyDesc.add(new exprNodeColumnDesc(valueInfo.getType(), valueInfo.getInternalName()));
          if (outputRS.get(key, field) == null)
            outputRS.put(key, field, new ColumnInfo((Integer.valueOf(outputPos++)).toString(), 
                                                    valueInfo.getType()));
        }
      }

      exprMap.put(tag, keyDesc);
      rightOps[pos++] = input.getOp();
    }

    org.apache.hadoop.hive.ql.plan.joinCond[] joinCondns = new org.apache.hadoop.hive.ql.plan.joinCond[join.getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      joinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new org.apache.hadoop.hive.ql.plan.joinCond(condn);
    }

    return new OperatorInfo(OperatorFactory.getAndMakeChild(new joinDesc(exprMap, joinCondns),
      new RowSchema(outputRS.getColumnInfos()), rightOps), outputRS);
  }

  @SuppressWarnings("nls")
  private OperatorInfo genJoinReduceSinkChild(QB qb, QBJoinTree joinTree,
      OperatorInfo child, String srcName, int pos) throws SemanticException {
    RowResolver inputRS = child.getRowResolver();
    RowResolver outputRS = new RowResolver();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    // Compute join keys and store in reduceKeys
    Vector<CommonTree> exprs = joinTree.getExpressions().get(pos);
    for (int i = 0; i < exprs.size(); i++) {
      CommonTree expr = exprs.get(i);
      reduceKeys.add(genExprNodeDesc(expr, srcName, inputRS));
    }

    // Walk over the input row resolver and copy in the output
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    Iterator<String> tblNamesIter = inputRS.getTableNames().iterator();
    while (tblNamesIter.hasNext())
    {
      String src = tblNamesIter.next();
      HashMap<String, ColumnInfo> fMap = inputRS.getFieldMap(src);
      for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
        String field = entry.getKey();
        ColumnInfo valueInfo = entry.getValue();
        reduceValues.add(new exprNodeColumnDesc(valueInfo.getType(), valueInfo.getInternalName()));
        if (outputRS.get(src, field) == null)
          outputRS.put(src, field,
                       new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." +
                                      Integer.valueOf(reduceValues.size() - 1).toString(),
                                      valueInfo.getType()));
      }
    }

    return new OperatorInfo(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, joinTree.getNextTag(), reduceKeys.size(), -1, false), 
        new RowSchema(outputRS.getColumnInfos()),
        child.getOp()), outputRS);
  }

  private OperatorInfo genJoinOperator(QB qb, QBJoinTree joinTree,
      HashMap<String, OperatorInfo> map) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    OperatorInfo joinSrcOp = null;
    if (leftChild != null)
    {
      OperatorInfo joinOp = genJoinOperator(qb, leftChild, map);
      joinSrcOp = genJoinReduceSinkChild(qb, joinTree, joinOp, null, 0);
    }

    OperatorInfo[] srcOps = new OperatorInfo[joinTree.getBaseSrc().length];
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        OperatorInfo srcOp = map.get(src);
        srcOps[pos] = genJoinReduceSinkChild(qb, joinTree, srcOp, src, pos);
        pos++;
      } else {
        assert pos == 0;
        srcOps[pos++] = null;
      }
    }

    // Type checking and implicit type conversion for join keys
    genJoinOperatorTypeCheck(joinSrcOp, srcOps);
    
    return genJoinOperatorChildren(joinTree, joinSrcOp, srcOps);
  }

  private void genJoinOperatorTypeCheck(OperatorInfo left, OperatorInfo[] right) throws SemanticException {
    // keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key list 
    ArrayList<ArrayList<exprNodeDesc>> keys = new ArrayList<ArrayList<exprNodeDesc>>();
    int keyLength = 0;
    for (int i=0; i<right.length; i++) {
      OperatorInfo oi = (i==0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator)(oi.getOp())).getConf();
      if (i == 0) {
        keyLength = now.getKeyCols().size();
      } else {
        assert(keyLength == now.getKeyCols().size());
      }
      keys.add(now.getKeyCols());
    }
    // implicit type conversion hierarchy
    for (int k = 0; k < keyLength; k++) {
      // Find the common class for type conversion
      Class<?> commonClass = keys.get(0).get(k).getTypeInfo().getPrimitiveClass();
      for(int i=1; i<right.length; i++) {
        Class<?> a = commonClass;
        Class<?> b = keys.get(i).get(k).getTypeInfo().getPrimitiveClass(); 
        commonClass = FunctionRegistry.getCommonClass(a, b);
        if (commonClass == null) {
          throw new SemanticException("Cannot do equality join on different types: " + a.getClass() + " and " + b.getClass());
        }
      }
      // Add implicit type conversion if necessary
      for(int i=0; i<right.length; i++) {
        if (!commonClass.isAssignableFrom(keys.get(i).get(k).getTypeInfo().getPrimitiveClass())) {
          keys.get(i).set(k, getFuncExprNodeDesc(commonClass.getName(), keys.get(i).get(k)));
        }
      }
    }
  }
  
  private OperatorInfo genJoinPlan(QB qb, HashMap<String, OperatorInfo> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    OperatorInfo joinOp = genJoinOperator(qb, joinTree, map);
    return joinOp;
  }

  private QBJoinTree genJoinTree(CommonTree joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinCond[] condn = new joinCond[1];

    if (joinParseTree.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
    {
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.LEFTOUTER);
    }
    else if (joinParseTree.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
    {
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.RIGHTOUTER);
    }
    else if (joinParseTree.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN)
    {
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.FULLOUTER);
    }
    else
    {
      condn[0] = new joinCond(0, 1, joinType.INNER);
      joinTree.setNoOuterJoin(true);
    }

    joinTree.setJoinCond(condn);

    CommonTree left = (CommonTree) joinParseTree.getChild(0);
    CommonTree right = (CommonTree) joinParseTree.getChild(1);

    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String table_name = left.getChild(0).getText();
      String alias = left.getChildCount() == 1 ? table_name : left.getChild(1)
          .getText();
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
    }
    else if (isJoinToken(left)) {
      QBJoinTree leftTree = genJoinTree(left);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++)
        leftAliases[i] = leftChildAliases[i];
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else
      assert (false);

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String table_name = right.getChild(0).getText();
      String alias = right.getChildCount() == 1 ? table_name : right.getChild(1)
          .getText();
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null)
        children = new String[2];
      children[1] = alias;
      joinTree.setBaseSrc(children);
    } else
      assert false;

    Vector<Vector<CommonTree>> expressions = new Vector<Vector<CommonTree>>();
    expressions.add(new Vector<CommonTree>());
    expressions.add(new Vector<CommonTree>());
    joinTree.setExpressions(expressions);
    CommonTree joinCond = (CommonTree) joinParseTree.getChild(2);
    assert joinCond != null;
    Vector<String> leftSrc = new Vector<String>();
    parseJoinCondition(joinParseTree, joinTree, joinCond, leftSrc);
    if (leftSrc.size() == 1)
      joinTree.setLeftAlias(leftSrc.get(0));

    return joinTree;
  }

  private void mergeJoins(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target, int pos) {
    String[] nodeRightAliases = node.getRightAliases();
    String[] trgtRightAliases = target.getRightAliases();
    String[] rightAliases = new String[nodeRightAliases.length
        + trgtRightAliases.length];

    for (int i = 0; i < trgtRightAliases.length; i++)
      rightAliases[i] = trgtRightAliases[i];
    for (int i = 0; i < nodeRightAliases.length; i++)
      rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
    target.setRightAliases(rightAliases);

    String[] nodeBaseSrc = node.getBaseSrc();
    String[] trgtBaseSrc = target.getBaseSrc();
    String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length - 1];

    for (int i = 0; i < trgtBaseSrc.length; i++)
      baseSrc[i] = trgtBaseSrc[i];
    for (int i = 1; i < nodeBaseSrc.length; i++)
      baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
    target.setBaseSrc(baseSrc);

    Vector<Vector<CommonTree>> expr = target.getExpressions();
    for (int i = 0; i < nodeRightAliases.length; i++)
      expr.add(node.getExpressions().get(i + 1));

    if (qb.getQbJoinTree() == node)
      qb.setQbJoinTree(node.getJoinSrc());
    else
      parent.setJoinSrc(node.getJoinSrc());

    if (node.getNoOuterJoin() && target.getNoOuterJoin())
      target.setNoOuterJoin(true);
    else
      target.setNoOuterJoin(false);

    joinCond[] nodeCondns = node.getJoinCond();
    int nodeCondnsSize = nodeCondns.length;
    joinCond[] targetCondns = target.getJoinCond();
    int targetCondnsSize = targetCondns.length;
    joinCond[] newCondns = new joinCond[nodeCondnsSize + targetCondnsSize];
    for (int i = 0; i < targetCondnsSize; i++)
      newCondns[i] = targetCondns[i];

    for (int i = 0; i < nodeCondnsSize; i++)
    {
      joinCond nodeCondn = nodeCondns[i];
      if (nodeCondn.getLeft() == 0)
        nodeCondn.setLeft(pos);
      else
        nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize - 1);
      nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize - 1);
      newCondns[targetCondnsSize + i] = nodeCondn;
    }

    target.setJoinCond(newCondns);
  }

  private int findMergePos(QBJoinTree node, QBJoinTree target) {
    int res = -1;
    String leftAlias = node.getLeftAlias();
    if (leftAlias == null)
      return -1;

    Vector<CommonTree> nodeCondn = node.getExpressions().get(0);
    Vector<CommonTree> targetCondn = null;

    if (leftAlias.equals(target.getLeftAlias()))
    {
      targetCondn = target.getExpressions().get(0);
      res = 0;
    }
    else
      for (int i = 0; i < target.getRightAliases().length; i++) {
        if (leftAlias.equals(target.getRightAliases()[i])) {
          targetCondn = target.getExpressions().get(i + 1);
          res = i + 1;
          break;
        }
      }

    if ((targetCondn == null) || (nodeCondn.size() != targetCondn.size()))
      return -1;

    for (int i = 0; i < nodeCondn.size(); i++)
      if (!nodeCondn.get(i).toStringTree().equals(
          targetCondn.get(i).toStringTree()))
        return -1;

    return res;
  }

  private boolean mergeJoinNodes(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target) {
    if (target == null)
      return false;

    int res = findMergePos(node, target);
    if (res != -1) {
      mergeJoins(qb, parent, node, target, res);
      return true;
    }

    return mergeJoinNodes(qb, parent, node, target.getJoinSrc());
  }

  private void mergeJoinTree(QB qb) {
    QBJoinTree root = qb.getQbJoinTree();
    QBJoinTree parent = null;
    while (root != null) {
      boolean merged = mergeJoinNodes(qb, parent, root, root.getJoinSrc());

      if (parent == null) {
        if (merged)
          root = qb.getQbJoinTree();
        else {
          parent = root;
          root = root.getJoinSrc();
        }
      } else {
        parent = parent.getJoinSrc();
        root = parent.getJoinSrc();
      }
    }
  }

  @SuppressWarnings("nls")
  private OperatorInfo genBodyPlan(QB qb, OperatorInfo input)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    String distinctText = null;
    CommonTree distn = null;
    OperatorInfo op = null;
    boolean grpBy = false;
    int     numGrpBy = 0;

    // In case of a multiple group bys, all of them should have the same distinct key
    for (String dest : ks) {
    	// is it a group by
      if ((qbp.getAggregationExprsForClause(dest).size() != 0)
          || (getGroupByForClause(qbp, dest).size() > 0)) {
        grpBy = true;
        numGrpBy++;

        // If there is a distinctFuncExp, add all parameters to the reduceKeys.
        if (qbp.getDistinctFuncExprForClause(dest) != null) {
          CommonTree value = qbp.getDistinctFuncExprForClause(dest);
          if (value.getChildCount() != 2)
            throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg(value));
          distn = (CommonTree)value.getChild(1);
          String dist = distn.toStringTree();;
          if (distinctText == null)
            distinctText = dist;
          if (!distinctText.equals(dist))
            throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg(value));
        }
      }
    }

    // In the first stage, copy the input and all the group by expressions
    // and aggregate paramaters. This can be optimized in the future to only
    // evaluate expressions that occur frequently. For a single groupby, no need to do so
    if (grpBy && (numGrpBy > 1)) {
      OperatorInfo reduceSinkOperatorInfo = 
        genGroupByPlanReduceSinkOperator(qbp, input, distn, ks);
      
      // ////// 2. Generate GroupbyOperator
      OperatorInfo forwardOperatorInfo = genGroupByPlanForwardOperator(qbp, reduceSinkOperatorInfo);
      op = forwardOperatorInfo;
    }

    // Go over all the destination tables
    OperatorInfo curr = null;
    for (String dest : ks) {
      boolean groupByExpr = false;
      if (qbp.getAggregationExprsForClause(dest).size() != 0
          || getGroupByForClause(qbp, dest).size() > 0) 
        groupByExpr = true;

      curr = input;      
      if (groupByExpr && (numGrpBy > 1)) 
        curr = op;

      if (qbp.getWhrForClause(dest) != null) {
        curr = genFilterPlan(dest, qb, curr);
      }

      if (qbp.getAggregationExprsForClause(dest).size() != 0
          || getGroupByForClause(qbp, dest).size() > 0) {
        if (numGrpBy > 1)
          curr = genGroupByPlan3MR(dest, qb, curr);
        else
          curr = genGroupByPlan2MR(dest, qb, curr);
      }
      
      curr = genSelectPlan(dest, qb, curr);
      Integer limit = qbp.getDestLimit(dest);

      if (qbp.getIsSubQ()) {
        if (qbp.getClusterByForClause(dest) != null)
          curr = genReduceSinkPlan(dest, qb, curr, -1);
        if (limit != null) 
          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), false);
      }
      else
      {
        curr = genConversionOps(dest, qb, curr);
        // exact limit can be taken care of by the fetch operator
        if (limit != null) {
          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), true);
          qb.getParseInfo().setOuterQueryLimit(limit.intValue());
        }
        curr = genFileSinkPlan(dest, qb, curr);
      }
    }

    LOG.debug("Created Body Plan for Query Block " + qb.getId());
    return curr;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genUnionPlan(String unionalias, String leftalias,
      OperatorInfo leftOp, String rightalias, OperatorInfo rightOp)
      throws SemanticException {

    RowResolver leftRR = leftOp.getRowResolver();
    RowResolver rightRR = rightOp.getRowResolver();
    HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
    HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
    // make sure the schemas of both sides are the same
    for (Map.Entry<String, ColumnInfo> lEntry: leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      ColumnInfo rInfo = rightmap.get(field);
      if (rInfo == null) {
        throw new SemanticException("Schema of both sides of union should match. "
            + rightalias + " does not have the field " + field);
      }
      if (lInfo == null) {
        throw new SemanticException("Schema of both sides of union should match. " 
            + leftalias + " does not have the field " + field);
      }
      if (!lInfo.getInternalName().equals(rInfo.getInternalName())) {
        throw new SemanticException("Schema of both sides of union should match: "
            + field + ":" + lInfo.getInternalName() + " " + rInfo.getInternalName());
      }
    }

    // construct the forward operator
    RowResolver unionoutRR = new RowResolver();
    for (Map.Entry<String, ColumnInfo> lEntry: leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      unionoutRR.put(unionalias, field, lInfo);
    }
    Operator<? extends Serializable> unionforward = OperatorFactory.get(forwardDesc.class,
        new RowSchema(unionoutRR.getColumnInfos()));
    // set forward operator as child of each of leftOp and rightOp
    List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    rightOp.getOp().setChildOperators(child);
    leftOp.getOp().setChildOperators(child);
    // create operator info list to return
    OperatorInfo unionout = new OperatorInfo(unionforward, unionoutRR);
    return unionout;
  }

  private exprNodeDesc genSamplePredicate(TableSample ts) {
    // ((default_sample_hashfn(cols) & Integer.MAX_VALUE) % denominator) == numerator
    exprNodeDesc numeratorExpr = new exprNodeConstantDesc(
        TypeInfoFactory.getPrimitiveTypeInfo(Integer.class), 
        Integer.valueOf(ts.getNumerator() - 1));
      
    exprNodeDesc denominatorExpr = new exprNodeConstantDesc(
        TypeInfoFactory.getPrimitiveTypeInfo(Integer.class), 
        Integer.valueOf(ts.getDenominator()));

    exprNodeDesc intMaxExpr = new exprNodeConstantDesc(
        TypeInfoFactory.getPrimitiveTypeInfo(Integer.class), 
        Integer.valueOf(Integer.MAX_VALUE));
    ArrayList<exprNodeDesc> args = new ArrayList<exprNodeDesc>();
    for (String col: ts.getCols()) {
      // TODO: change type to the one in the table schema
      args.add(new exprNodeColumnDesc(String.class, col));
    }

    exprNodeDesc hashfnExpr = getFuncExprNodeDesc("default_sample_hashfn", args);
    assert(hashfnExpr != null);
    LOG.info("hashfnExpr = " + hashfnExpr);
    exprNodeDesc andExpr = getFuncExprNodeDesc("&", hashfnExpr, intMaxExpr);
    assert(andExpr != null);
    LOG.info("andExpr = " + andExpr);
    exprNodeDesc modExpr = getFuncExprNodeDesc("%", andExpr, denominatorExpr);
    assert(modExpr != null);
    LOG.info("modExpr = " + modExpr);
    LOG.info("numeratorExpr = " + numeratorExpr);
    exprNodeDesc equalsExpr = getFuncExprNodeDesc("==", modExpr, numeratorExpr);
    LOG.info("equalsExpr = " + equalsExpr);
    assert(equalsExpr != null);
    return equalsExpr;
  }
  
  @SuppressWarnings("nls")
  private OperatorInfo genTablePlan(String alias, QB qb)
      throws SemanticException {

    Table tab = qb.getMetaData().getSrcForAlias(alias);

    RowResolver rwsch = new RowResolver();
    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector)tab.getDeserializer().getObjectInspector();
      List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
      for (int i=0; i<fields.size(); i++) {
        rwsch.put(alias, fields.get(i).getFieldName(),
            new ColumnInfo(fields.get(i).getFieldName(), 
                TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector())));
      }
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }
    // Hack!! - refactor once the metadata APIs with types are ready
    // Finally add the partitioning columns
    for(FieldSchema part_col: tab.getPartCols()) {
      LOG.trace("Adding partition col: " + part_col);
      // TODO: use the right type by calling part_col.getType() instead of String.class
      rwsch.put(alias, part_col.getName(), new ColumnInfo(part_col.getName(), String.class));
    }

    // Create the root of the operator tree
    Operator<? extends Serializable> top = OperatorFactory.get(forwardDesc.class,
        new RowSchema(rwsch.getColumnInfos()));
    String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);

    // Add this to the list of top operators - we always start from a table scan
    this.topOps.put(alias_id, top);
    
    // check if this table is sampled and needs more than input pruning
    Operator<? extends Serializable> tableOp = top;
    TableSample ts = qb.getParseInfo().getTabSample(alias);
    if (ts != null) {
      int num = ts.getNumerator();
      int den = ts.getDenominator();
      ArrayList<String> sampleCols = ts.getCols();
      List<FieldSchema> tabCols = tab.getCols();
      // check if sampleCols are present in the table
      for (String col: sampleCols) {
        boolean found = false;
        for (FieldSchema s: tabCols) {
          if (col.equalsIgnoreCase(s.getName())) {
            found = true;
          }
        }
        if (!found) {
          throw new SemanticException(ErrorMsg.SAMPLE_COLUMN_NOT_FOUND.getMsg(
              qb.getParseInfo().getSrcForAlias(alias), "Sampling column " + 
              col + " not found in table " + tab.getName()));
        }
      }
      List<String> tabBucketCols = tab.getBucketCols();
      int numBuckets = tab.getNumBuckets();
      // check if a predicate is needed
      // predicate is needed if either input pruning is not enough
      // or if input pruning is not possible
      
      // check if the sample columns are the same as the table bucket columns
      // and if they are, create a new array of column names which is in the
      // same order as tabBucketCols.
      // if sample cols is not specified then default is bucket cols
      boolean colsEqual = true;
      if ( (sampleCols.size() != tabBucketCols.size()) && (sampleCols.size() != 0) ) {
        colsEqual = false;
      }
      for (int i = 0; i < sampleCols.size() && colsEqual; i++) {
        boolean colFound = false;
        for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
          if (sampleCols.get(i).equalsIgnoreCase(tabBucketCols.get(j))) {
            colFound = true;
          }
        }
        colsEqual = colFound;
      }
      // if the sample columns are the same, we need them in the same order
      // as tabBucketCols
      if (colsEqual) {
        ts.setCols(new ArrayList<String>(tabBucketCols));
      }
      
      // check if input pruning is enough     
      if ((sampleCols == null || sampleCols.size() == 0 || colsEqual)
          && (num == den || den <= numBuckets && numBuckets % den == 0)) { 
        // input pruning is enough; no need for filter
        LOG.info("No need for sample filter");
      } 
      else {
        // need to add filter
        // create tableOp to be filterDesc and set as child to 'top'
        LOG.info("Need sample filter");
        exprNodeDesc samplePredicate = genSamplePredicate(ts);
        tableOp = OperatorFactory.getAndMakeChild(
            new filterDesc(samplePredicate), 
            top);
      }
    }
    OperatorInfo output = new OperatorInfo(tableOp, rwsch);

    LOG.debug("Created Table Plan for " + alias + " " + tableOp.toString());

    return output;
  }

  private OperatorInfo genPlan(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      return genPlan(qbexpr.getQB());
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
      OperatorInfo qbexpr1Ops = genPlan(qbexpr.getQBExpr1());
      OperatorInfo qbexpr2Ops = genPlan(qbexpr.getQBExpr2());

      return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(),
          qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    return null;
  }

  @SuppressWarnings("nls")
  private OperatorInfo genPlan(QB qb) throws SemanticException {

    // First generate all the opInfos for the elements in the from clause
    HashMap<String, OperatorInfo> aliasToOpInfo = new HashMap<String, OperatorInfo>();

    // Recurse over the subqueries to fill the subquery part of the plan
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      aliasToOpInfo.put(alias, genPlan(qbexpr));
      qbexpr.setAlias(alias);
    }

    // Recurse over all the source tables
    for (String alias : qb.getTabAliases()) {
      aliasToOpInfo.put(alias, genTablePlan(alias, qb));
    }

    OperatorInfo srcOpInfo = null;

    // process join
    if (qb.getParseInfo().getJoinExpr() != null) {
      CommonTree joinExpr = qb.getParseInfo().getJoinExpr();
      QBJoinTree joinTree = genJoinTree(joinExpr);
      qb.setQbJoinTree(joinTree);
      mergeJoinTree(qb);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    }
    else
      // Now if there are more than 1 sources then we have a join case
      // later we can extend this to the union all case as well
      srcOpInfo = aliasToOpInfo.values().iterator().next();

    OperatorInfo bodyOpInfo = genBodyPlan(qb, srcOpInfo);
    LOG.debug("Created Plan for Query Block " + qb.getId());

    // is it a top level QB, and can it be optimized ? For eg: select * from T does not need a map-reduce job
    QBParseInfo qbp = qb.getParseInfo();
    qbp.setCanOptTopQ(qb.isSelectStarQuery());
      	
    return bodyOpInfo;
  }

  private Operator<? extends Serializable> getReduceSink(Operator<? extends Serializable> top) {
    if (top.getClass() == ReduceSinkOperator.class) {
      // Get the operator following the reduce sink
      assert (top.getChildOperators().size() == 1);

      return top;
    }

    List<Operator<? extends Serializable>> childOps = top.getChildOperators();
    if (childOps == null) {
      return null;
    }

    for (int i = 0; i < childOps.size(); ++i) {
      Operator<? extends Serializable> reducer = getReduceSink(childOps.get(i));
      if (reducer != null) {
        return reducer;
      }
    }

    return null;
  }

  @SuppressWarnings("nls")
  private void genMapRedTasks(QB qb) throws SemanticException {
    fetchWork fetch = null;
    moveWork  mv = null;
    Task<? extends Serializable> mvTask = null;
    Task<? extends Serializable> fetchTask = null;

    if (qb.getParseInfo().getCanOptTopQ()) {
      Iterator<Map.Entry<String, Table>> iter = qb.getMetaData().getAliasToTable().entrySet().iterator();
      Table tab = ((Map.Entry<String, Table>)iter.next()).getValue();
      fetch = new fetchWork(tab.getPath(), tab.getDeserializer().getClass(),
                            tab.getInputFormatClass(), tab.getSchema(), qb.getParseInfo().getOuterQueryLimit());    

      fetchTask = TaskFactory.get(fetch, this.conf);
      setFetchTask(fetchTask);
      return;
    }

    // In case of a select, use a fetch task instead of a move task
    if (qb.getIsQuery()) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1))
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      String cols = loadFileWork.get(0).getColumns();
      fetch = new fetchWork(new Path(loadFileWork.get(0).getSourceDir()), 
                            MetadataTypedColumnsetSerDe.class, TextInputFormat.class,
                            Utilities.makeProperties("columns", cols), qb.getParseInfo().getOuterQueryLimit());    

      fetchTask = TaskFactory.get(fetch, this.conf);
      setFetchTask(fetchTask);
    }
    else {
      // First we generate the move work as this needs to be made dependent on all
      // the tasks that have a file sink operation
      mv = new moveWork(loadTableWork, loadFileWork);
      mvTask = TaskFactory.get(mv, this.conf);
    }

    // Maintain a map from the top level left most reducer in each of these
    // trees
    // to a task. This tells us whether we have to allocate another
    // root level task or we can reuse an existing one
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = 
        new HashMap<Operator<? extends Serializable>, Task<? extends Serializable>>();
    for (String alias_id : this.topOps.keySet()) {
      Operator<? extends Serializable> topOp = this.topOps.get(alias_id);
      Operator<? extends Serializable> reduceSink = getReduceSink(topOp);
      Operator<? extends Serializable> reducer = null;
      if (reduceSink != null) 
        reducer = reduceSink.getChildOperators().get(0);      
      Task<? extends Serializable> rootTask = opTaskMap.get(reducer);
      if (rootTask == null) {
        rootTask = TaskFactory.get(getMapRedWork(), this.conf);
        opTaskMap.put(reducer, rootTask);
        ((mapredWork) rootTask.getWork()).setReducer(reducer);
        reduceSinkDesc desc = (reduceSink == null) ? null : (reduceSinkDesc)reduceSink.getConf();

        // The number of reducers may be specified in the plan in some cases, or may need to be inferred
        if (desc != null) {
          if (desc.getNumReducers() != -1)
            ((mapredWork) rootTask.getWork()).setNumReduceTasks(new Integer(desc.getNumReducers()));
          else if (desc.getInferNumReducers() == true)
            ((mapredWork) rootTask.getWork()).setInferNumReducers(true);
        }
        this.rootTasks.add(rootTask);
      }
      genTaskPlan(topOp, rootTask, opTaskMap, mvTask);

      // Generate the map work for this alias_id
      PartitionPruner pruner = this.aliasToPruner.get(alias_id);
      Set<Partition> parts = null;
      try {
        parts = pruner.prune();
      } catch (HiveException e) {
        // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }
      SamplePruner samplePruner = this.aliasToSamplePruner.get(alias_id);
      mapredWork plan = (mapredWork) rootTask.getWork();
      for (Partition part : parts) {
        // Later the properties have to come from the partition as opposed
        // to from the table in order to support versioning.
        Path paths[];
        if (samplePruner != null) {
          paths = samplePruner.prune(part);
        }
        else {
          paths = part.getPath();
        }
        for (Path p: paths) {
          String path = p.toString();
          LOG.debug("Adding " + path + " of table" + alias_id);
          // Add the path to alias mapping
          if (plan.getPathToAliases().get(path) == null) {
            plan.getPathToAliases().put(path, new ArrayList<String>());
          }
          plan.getPathToAliases().get(path).add(alias_id);
          plan.getPathToPartitionInfo().put(path, Utilities.getPartitionDesc(part));
          LOG.debug("Information added for path " + path);
        }
      }
      plan.getAliasToWork().put(alias_id, topOp);
      LOG.debug("Created Map Work for " + alias_id);
    }
  }

  @SuppressWarnings("nls")
  private void genTaskPlan(Operator<? extends Serializable> op, Task<? extends Serializable> currTask,
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> redTaskMap, 
      Task<? extends Serializable> mvTask) {
    // Check if this is a file sink operator
    if ((op.getClass() == FileSinkOperator.class) && (mvTask != null)) { 
      // If this is a file sink operator then set the move task to be dependent
      // on the current task
      currTask.addDependentTask(mvTask);
    }

    List<Operator<? extends Serializable>> childOps = op.getChildOperators();

    // If there are no children then we are done
    if (childOps == null) {
      return;
    }

    // Otherwise go through the children and check for the operator following
    // the reduce sink operator
    mapredWork plan = (mapredWork) currTask.getWork();
    for (int i = 0; i < childOps.size(); ++i) {
      Operator<? extends Serializable> child = childOps.get(i);
      
      if (child.getClass() == ReduceSinkOperator.class) {
        // Get the operator following the reduce sink
        assert (child.getChildOperators().size() == 1);

        Operator<? extends Serializable> reducer = child.getChildOperators().get(0);
        assert (plan.getReducer() != null);
        if (plan.getReducer() == reducer) {
          if (child.getChildOperators().get(0).getClass() == JoinOperator.class)
            plan.setNeedsTagging(true);

          // Recurse on the reducer
          genTaskPlan(reducer, currTask, redTaskMap, mvTask);
        }
        else if (plan.getReducer() != reducer) {
          Task<? extends Serializable> ctask = null;
          mapredWork cplan = null;

          // First check if the reducer already has an associated task
          ctask = redTaskMap.get(reducer);
          if (ctask == null) {
            // For this case we need to generate a new task
            cplan = getMapRedWork();
            ctask = TaskFactory.get(cplan, this.conf);
            // Add the reducer
            cplan.setReducer(reducer);
            if (((reduceSinkDesc)child.getConf()).getNumReducers() != -1)
              cplan.setNumReduceTasks(new Integer(((reduceSinkDesc)child.getConf()).getNumReducers()));
            else
              cplan.setInferNumReducers(((reduceSinkDesc)child.getConf()).getInferNumReducers());
            redTaskMap.put(reducer, ctask);

            // Recurse on the reducer
            genTaskPlan(reducer, ctask, redTaskMap, mvTask);

            // generate the temporary file
            String taskTmpDir = this.scratchDir + File.separator + this.randomid + '.' + this.pathid ;
            this.pathid++;

            // Go over the row schema of the input operator and generate the
            // column names using that
            StringBuilder sb = new StringBuilder();
            boolean isfirst = true;
            for(ColumnInfo colInfo: op.getSchema().getSignature()) {
              if (!isfirst) {
                sb.append(",");
              }
              sb.append(colInfo.getInternalName());
              isfirst = false;
            }

            tableDesc tt_desc = PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.ctrlaCode),
                                         sb.toString());

            // Create a file sink operator for this file name
            Operator<? extends Serializable> fs_op = OperatorFactory.get(new fileSinkDesc(taskTmpDir, tt_desc),
                                                                         op.getSchema());

            // replace the reduce child with this operator
            childOps.set(i, fs_op);

            // Add the path to alias mapping
            if (cplan.getPathToAliases().get(taskTmpDir) == null) {
              cplan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
            }

            String streamDesc;
            if (child.getChildOperators().get(0).getClass() == JoinOperator.class)
              streamDesc = "$INTNAME";
            else
              streamDesc = taskTmpDir;


            cplan.getPathToAliases().get(taskTmpDir).add(streamDesc);

            cplan.getPathToPartitionInfo().put(taskTmpDir,
                                               new partitionDesc(tt_desc, null));

            cplan.getAliasToWork().put(streamDesc, child);

            // Make this task dependent on the current task
            currTask.addDependentTask(ctask);

            // TODO: Allocate work to remove the temporary files and make that
            // dependent on the cTask
            if (child.getChildOperators().get(0).getClass() == JoinOperator.class)
              cplan.setNeedsTagging(true);
          }
        }
        child.setChildOperators(null);

      } else {
        // For any other operator just recurse
        genTaskPlan(child, currTask, redTaskMap, mvTask);
      }
    }
  }

  @SuppressWarnings("nls")
  private Phase1Ctx initPhase1Ctx() {

    Phase1Ctx ctx_1 = new Phase1Ctx();
    ctx_1.nextNum = 0;
    ctx_1.dest = "reduce";

    return ctx_1;
  }

  private mapredWork getMapRedWork() {

    mapredWork work = new mapredWork();
    work.setPathToAliases(new LinkedHashMap<String, ArrayList<String>>());
    work.setPathToPartitionInfo(new LinkedHashMap<String, partitionDesc>());
    work.setAliasToWork(new HashMap<String, Operator<? extends Serializable>>());
    work.setReducer(null);

    return work;
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(CommonTree ast, Context ctx) throws SemanticException {
    this.ctx = ctx;
    reset();

    QB qb = new QB(null, null, false);

    LOG.info("Starting Semantic Analysis");
    doPhase1(ast, qb, initPhase1Ctx());
    LOG.info("Completed phase 1 of Semantic Analysis");

    getMetaData(qb);
    LOG.info("Completed getting MetaData in Semantic Analysis");

    genPlan(qb);

    // Do any partition pruning
    genPartitionPruners(qb);
    LOG.info("Completed partition pruning");

    // Do any sample pruning
    genSamplePruners(qb);
    LOG.info("Completed sample pruning");
    
    // TODO - this can be extended to create multiple
    // map reduce plans later

    // At this point we have the complete operator tree
    // from which we want to find the reduce operator
    genMapRedTasks(qb);

    LOG.info("Completed plan generation");

    return;
  }
  
  /**
   * Get the exprNodeDesc
   * @param name
   * @param children
   * @return
   */
  public static exprNodeDesc getFuncExprNodeDesc(String name, exprNodeDesc... children) {
    return getFuncExprNodeDesc(name, Arrays.asList(children));
  }
  
  /**
   * This function create an ExprNodeDesc for a UDF function given the children (arguments).
   * It will insert implicit type conversion functions if necessary. 
   * @throws SemanticException 
   */
  public static exprNodeDesc getFuncExprNodeDesc(String udfName, List<exprNodeDesc> children) {
    // Find the corresponding method
    ArrayList<Class<?>> argumentClasses = new ArrayList<Class<?>>(children.size());
    for(int i=0; i<children.size(); i++) {
      exprNodeDesc child = children.get(i);
      assert(child != null);
      TypeInfo childTypeInfo = child.getTypeInfo();
      assert(childTypeInfo != null);
      
      // Note: we don't pass the element types of MAP/LIST to UDF.
      // That will work for null test and size but not other more complex functionalities like list slice etc.
      // For those more complex functionalities, we plan to have a ComplexUDF interface which has an evaluate
      // method that accepts a list of objects and a list of objectinspectors. 
      switch (childTypeInfo.getCategory()) {
        case PRIMITIVE: {
          argumentClasses.add(childTypeInfo.getPrimitiveClass());
          break;
        }
        case MAP: {
          argumentClasses.add(Map.class);
          break;
        }
        case LIST: {
          argumentClasses.add(List.class);
          break;
        }
        case STRUCT: {
          argumentClasses.add(Object.class);
          break;
        }
        default: {
          // should never happen
          assert(false);
        }
      }
    }
    Method udfMethod = FunctionRegistry.getUDFMethod(udfName, false, argumentClasses);
    if (udfMethod == null) return null;

    ArrayList<exprNodeDesc> ch = new ArrayList<exprNodeDesc>();
    Class<?>[] pTypes = udfMethod.getParameterTypes();

    for (int i = 0; i < children.size(); i++)
    {
      exprNodeDesc desc = children.get(i);
      Class<?> pType = ObjectInspectorUtils.generalizePrimitive(pTypes[i]);
      if (desc instanceof exprNodeNullDesc) {
        exprNodeConstantDesc newCh = new exprNodeConstantDesc(TypeInfoFactory.getPrimitiveTypeInfo(pType), null);
        ch.add(newCh);
      } else if (pType.isAssignableFrom(argumentClasses.get(i))) {
        // no type conversion needed
        ch.add(desc);
      } else {
        // must be implicit type conversion
        Class<?> from = argumentClasses.get(i);
        Class<?> to = pType;
        assert(FunctionRegistry.implicitConvertable(from, to));
        Method m = FunctionRegistry.getUDFMethod(to.getName(), true, from);
        assert(m != null);
        Class<? extends UDF> c = FunctionRegistry.getUDFClass(to.getName());
        assert(c != null);

        // get the conversion method
        ArrayList<exprNodeDesc> conversionArg = new ArrayList<exprNodeDesc>(1);
        conversionArg.add(desc);
        ch.add(new exprNodeFuncDesc(
              TypeInfoFactory.getPrimitiveTypeInfo(pType),
              c, m, conversionArg));
      }
    }

    exprNodeFuncDesc desc = new exprNodeFuncDesc(
      TypeInfoFactory.getPrimitiveTypeInfo(udfMethod.getReturnType()),
      FunctionRegistry.getUDFClass(udfName),
      udfMethod, ch);
    return desc;
  }


  @SuppressWarnings("nls")
  private exprNodeDesc genExprNodeDesc(CommonTree expr, String alias, RowResolver input)
  throws SemanticException {
    //  We recursively create the exprNodeDesc.  Base cases:  when we encounter 
    //  a column ref, we convert that into an exprNodeColumnDesc;  when we encounter 
    //  a constant, we convert that into an exprNodeConstantDesc.  For others we just 
    //  build the exprNodeFuncDesc with recursively built children.
    
    exprNodeDesc desc = null;

    //  If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.get("", expr.toStringTree());
    if (colInfo != null) {
      desc = new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName()); 
      return desc;
    }

    //  Is this a simple expr node (not a TOK_COLREF or a TOK_FUNCTION or an operator)?
    desc = genSimpleExprNodeDesc(expr);
    if (desc != null) {
      return desc;
    }
    
    int tokType = expr.getType();
    switch (tokType) {
      case HiveParser.TOK_COLREF: {

        // For now only allow columns of the form tab.col
        if (expr.getChildCount() == 1) {
          throw new SemanticException(ErrorMsg.NO_TABLE_ALIAS.getMsg(expr.getChild(0)));
        }
        
        String tabAlias = expr.getChild(0).getText();
        String colName = expr.getChild(1).getText();
        if (tabAlias == null || colName == null) {
          throw new SemanticException(ErrorMsg.INVALID_XPATH.getMsg(expr));
        }
        colInfo = input.get(tabAlias, colName);

        if (colInfo == null && input.getIsExprResolver()) {
          throw new SemanticException(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(expr));
        }         
        else if (!input.hasTableAlias(tabAlias)) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(expr.getChild(0)));
        } else if (colInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)));
        }

        desc = new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName());
        break;
      }
  
      default: {
        boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);
        
        // Create all children
        int childrenBegin = (isFunction ? 1 : 0);
        ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(expr.getChildCount() - childrenBegin);
        for (int ci=childrenBegin; ci<expr.getChildCount(); ci++) {
          children.add(genExprNodeDesc((CommonTree)expr.getChild(ci), alias, input));
        }
        
        // Create function desc
        desc = getXpathOrFuncExprNodeDesc(expr, isFunction, children);
        break;
      }
    }
    assert(desc != null);
    return desc;
  }

  static HashMap<Integer, String> specialUnaryOperatorTextHashMap;
  static HashMap<Integer, String> specialFunctionTextHashMap;
  static HashMap<Integer, String> conversionFunctionTextHashMap;
  static {
    specialUnaryOperatorTextHashMap = new HashMap<Integer, String>();
    specialUnaryOperatorTextHashMap.put(HiveParser.PLUS, "positive");
    specialUnaryOperatorTextHashMap.put(HiveParser.MINUS, "negative");
    specialFunctionTextHashMap = new HashMap<Integer, String>();
    specialFunctionTextHashMap.put(HiveParser.TOK_ISNULL, "isnull");
    specialFunctionTextHashMap.put(HiveParser.TOK_ISNOTNULL, "isnotnull");
    conversionFunctionTextHashMap = new HashMap<Integer, String>();
    conversionFunctionTextHashMap.put(HiveParser.TOK_BOOLEAN, Boolean.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_TINYINT, Byte.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_INT, Integer.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_BIGINT, Long.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_FLOAT, Float.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_DOUBLE, Double.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_STRING, String.class.getName());
    conversionFunctionTextHashMap.put(HiveParser.TOK_DATE, java.sql.Date.class.getName());
  }
  
  public static boolean isRedundantConversionFunction(CommonTree expr, boolean isFunction, ArrayList<exprNodeDesc> children) {
    if (!isFunction) return false;
    // children is always one less than the expr.getChildCount(), since the latter contains function name.
    assert(children.size() == expr.getChildCount() - 1);
    // conversion functions take a single parameter
    if (children.size() != 1) return false;
    String funcText = conversionFunctionTextHashMap.get(((CommonTree)expr.getChild(0)).getType());
    // not a conversion function 
    if (funcText == null) return false;
    // return true when the child type and the conversion target type is the same
    return children.get(0).getTypeInfo().getPrimitiveClass().getName().equals(funcText);
  }
  
  public static String getFunctionText(CommonTree expr, boolean isFunction) {
    String funcText = null;
    if (!isFunction) {
      // For operator, the function name is the operator text, unless it's in our special dictionary
      if (expr.getChildCount() == 1) {
        funcText = specialUnaryOperatorTextHashMap.get(expr.getType());
      }
      if (funcText == null) {
        funcText = expr.getText();
      }
    } else {
      // For TOK_FUNCTION, the function name is stored in the first child, unless it's in our
      // special dictionary.
      assert(expr.getChildCount() >= 1);
      int funcType = ((CommonTree)expr.getChild(0)).getType();
      funcText = specialFunctionTextHashMap.get(funcType);
      if (funcText == null) {
        funcText = conversionFunctionTextHashMap.get(funcType);
      }
      if (funcText == null) {
        funcText = ((CommonTree)expr.getChild(0)).getText();
      }
    }
    return funcText;
  }
  
  static exprNodeDesc getXpathOrFuncExprNodeDesc(CommonTree expr, boolean isFunction,
      ArrayList<exprNodeDesc> children)
      throws SemanticException {
    // return the child directly if the conversion is redundant.
    if (isRedundantConversionFunction(expr, isFunction, children)) {
      assert(children.size() == 1);
      assert(children.get(0) != null);
      return children.get(0);
    }
    String funcText = getFunctionText(expr, isFunction);
    exprNodeDesc desc;
    if (funcText.equals(".")) {
      // "." :  FIELD Expression
      assert(children.size() == 2);
      // Only allow constant field name for now
      assert(children.get(1) instanceof exprNodeConstantDesc);
      exprNodeDesc object = children.get(0);
      exprNodeConstantDesc fieldName = (exprNodeConstantDesc)children.get(1);
      assert(fieldName.getValue() instanceof String);
      
      // Calculate result TypeInfo
      String fieldNameString = (String)fieldName.getValue();
      TypeInfo objectTypeInfo = object.getTypeInfo();
      
      // Allow accessing a field of list element structs directly from a list  
      boolean isList = (object.getTypeInfo().getCategory() == ObjectInspector.Category.LIST);
      if (isList) {
        objectTypeInfo = objectTypeInfo.getListElementTypeInfo();
      }
      if (objectTypeInfo.getCategory() != Category.STRUCT) {
        throw new SemanticException(ErrorMsg.INVALID_DOT.getMsg(expr));
      }
      TypeInfo t = objectTypeInfo.getStructFieldTypeInfo(fieldNameString);
      if (isList) {
        t = TypeInfoFactory.getListTypeInfo(t);
      }
      
      desc = new exprNodeFieldDesc(t, children.get(0), fieldNameString, isList);
      
    } else if (funcText.equals("[")){
      // "[]" : LSQUARE/INDEX Expression
      assert(children.size() == 2);
      
      // Check whether this is a list or a map
      TypeInfo myt = children.get(0).getTypeInfo();

      if (myt.getCategory() == Category.LIST) {
        // Only allow constant integer index for now
        if (!(children.get(1) instanceof exprNodeConstantDesc)
            || !(((exprNodeConstantDesc)children.get(1)).getValue() instanceof Integer)) {
          throw new SemanticException(ErrorMsg.INVALID_ARRAYINDEX_CONSTANT.getMsg(expr));
        }
      
        // Calculate TypeInfo
        TypeInfo t = myt.getListElementTypeInfo();
        desc = new exprNodeIndexDesc(t, children.get(0), children.get(1));
      }
      else if (myt.getCategory() == Category.MAP) {
        // Only allow only constant indexes for now
        if (!(children.get(1) instanceof exprNodeConstantDesc)) {
          throw new SemanticException(ErrorMsg.INVALID_MAPINDEX_CONSTANT.getMsg(expr));
        }
        if (!(((exprNodeConstantDesc)children.get(1)).getValue().getClass() == 
              myt.getMapKeyTypeInfo().getPrimitiveClass())) {
          throw new SemanticException(ErrorMsg.INVALID_MAPINDEX_TYPE.getMsg(expr));
        }
        // Calculate TypeInfo
        TypeInfo t = myt.getMapValueTypeInfo();
        
        desc = new exprNodeIndexDesc(t, children.get(0), children.get(1));
      }
      else {
        throw new SemanticException(ErrorMsg.NON_COLLECTION_TYPE.getMsg(expr));
      }
    } else {
      // other operators or functions
      Class<? extends UDF> udf = FunctionRegistry.getUDFClass(funcText);
      if (udf == null) {
      	if (isFunction)
          throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg((CommonTree)expr.getChild(0)));
      	else
          throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg((CommonTree)expr));
      }
      
      desc = getFuncExprNodeDesc(funcText, children);
      if (desc == null) {
        ArrayList<Class<?>> argumentClasses = new ArrayList<Class<?>>(children.size());
        for(int i=0; i<children.size(); i++) {
          argumentClasses.add(children.get(i).getTypeInfo().getPrimitiveClass());
        }
  
        if (isFunction) {
          String reason = "Looking for UDF \"" + expr.getChild(0).getText() + "\" with parameters " + argumentClasses;
          throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg((CommonTree)expr.getChild(0), reason));
        } else {
          String reason = "Looking for Operator \"" + expr.getText() + "\" with parameters " + argumentClasses;
          throw new SemanticException(ErrorMsg.INVALID_OPERATOR_SIGNATURE.getMsg(expr, reason));
        }
      }
    }
    // UDFOPPositive is a no-op.
    // However, we still create it, and then remove it here, to make sure we only allow
    // "+" for numeric types.
    if (desc instanceof exprNodeFuncDesc) {
      exprNodeFuncDesc funcDesc = (exprNodeFuncDesc)desc;
      if (funcDesc.getUDFClass().equals(UDFOPPositive.class)) {
        assert(funcDesc.getChildren().size() == 1);
        desc = funcDesc.getChildren().get(0);
      }
    }
    assert(desc != null);
    return desc;
  }

  static exprNodeDesc genSimpleExprNodeDesc(CommonTree expr) throws SemanticException {
    exprNodeDesc desc = null;
    switch(expr.getType()) {
      case HiveParser.TOK_NULL:
        desc = new exprNodeNullDesc();
        break;
      case HiveParser.Identifier:
        // This is the case for an XPATH element (like "c" in "a.b.c.d")
        desc = new exprNodeConstantDesc(
            TypeInfoFactory.getPrimitiveTypeInfo(String.class), expr.getText());
        break;
      case HiveParser.Number:
        Number v = null;
        try {
          v = Double.valueOf(expr.getText());
          v = Long.valueOf(expr.getText());
          v = Integer.valueOf(expr.getText());
        } catch (NumberFormatException e) {
          // do nothing here, we will throw an exception in the following block
        }
        if (v == null) {
          throw new SemanticException(ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(expr));
        }
        desc = new exprNodeConstantDesc(v);
        break;
      case HiveParser.StringLiteral:
        desc = new exprNodeConstantDesc(String.class, BaseSemanticAnalyzer.unescapeSQLString(expr.getText()));
        break;
      case HiveParser.TOK_CHARSETLITERAL:
        desc = new exprNodeConstantDesc(String.class, BaseSemanticAnalyzer.charSetString(expr.getChild(0).getText(), expr.getChild(1).getText()));
        break;
      case HiveParser.KW_TRUE:
        desc = new exprNodeConstantDesc(Boolean.class, Boolean.TRUE);
        break;
      case HiveParser.KW_FALSE:
        desc = new exprNodeConstantDesc(Boolean.class, Boolean.FALSE);
        break;
    }
    return desc;
  }
}
