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
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
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
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;
  private QB qb;
  private CommonTree ast;

  private static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(HiveConf conf) throws SemanticException {

    super(conf);

    this.aliasToPruner = new HashMap<String, PartitionPruner>();
    this.aliasToSamplePruner = new HashMap<String, SamplePruner>();
    this.topOps = new HashMap<String, Operator<? extends Serializable>>();
    this.topSelOps = new HashMap<String, Operator<? extends Serializable>>();
    this.loadTableWork = new ArrayList<loadTableDesc>();
    this.loadFileWork = new ArrayList<loadFileDesc>();
    opParseCtx = new HashMap<Operator<? extends Serializable>, OpParseContext>();
  }

  @Override
  protected void reset() {
    super.reset();
    this.aliasToPruner.clear();
    this.loadTableWork.clear();
    this.loadFileWork.clear();
    this.topOps.clear();
    this.topSelOps.clear();
    qb = null;
    ast = null;
  }

  public void init(ParseContext pctx) {
    aliasToPruner = pctx.getAliasToPruner();
    aliasToSamplePruner = pctx.getAliasToSamplePruner();
    topOps = pctx.getTopOps();
    topSelOps = pctx.getTopSelOps();
    opParseCtx = pctx.getOpParseCtx();
    loadTableWork = pctx.getLoadTableWork();
    loadFileWork = pctx.getLoadFileWork();
    ctx = pctx.getContext();
  }

  public ParseContext getParseContext() {
    return new ParseContext(conf, qb, ast, aliasToPruner, aliasToSamplePruner, topOps, 
                            topSelOps, opParseCtx, loadTableWork, loadFileWork, ctx);
  }
  
  @SuppressWarnings("nls")
  public void doPhase1QBExpr(CommonTree ast, QBExpr qbexpr, String id,
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
      String functionName = unescapeIdentifier(expressionTree.getChild(0).getText());
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
    String alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGOUS_TABLE_ALIAS.getMsg(tabref.getChild(aliasIndex)));
    }
    if (tableSamplePresent) {
      CommonTree sampleClause = (CommonTree)tabref.getChild(1);
      ArrayList<CommonTree> sampleCols = new ArrayList<CommonTree>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add((CommonTree)sampleClause.getChild(i));
        }
      }
      // TODO: For now only support sampling on up to two columns
      // Need to change it to list of columns
      if (sampleCols.size() > 2) {
        throw new SemanticException(ErrorMsg.SAMPLE_RESTRICTION.getMsg(tabref.getChild(0)));
      }
      qb.getParseInfo().setTabSample(alias, new TableSample(
          unescapeIdentifier(sampleClause.getChild(0).getText()), 
          unescapeIdentifier(sampleClause.getChild(1).getText()),
          sampleCols)
      );
    }
    // Insert this map into the stats
    String table_name = unescapeIdentifier(tabref.getChild(0).getText());
    qb.setTabAlias(alias, table_name);

    qb.getParseInfo().setSrcForAlias(alias, tableTree);
  }

  private void processSubQuery(QB qb, CommonTree subq) throws SemanticException {

    // This is a subquery and must have an alias
    if (subq.getChildCount() != 2) {
      throw new SemanticException(ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(subq));
    }
    CommonTree subqref = (CommonTree) subq.getChild(0);
    String alias = unescapeIdentifier(subq.getChild(1).getText());

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
  public void doPhase1(CommonTree ast, QB qb, Phase1Ctx ctx_1)
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

  /** 
   * Generate partition pruners. The filters can occur in the where clause and in the JOIN conditions. First, walk over the 
   * filters in the join condition and AND them, since all of them are needed. Then for each where clause, traverse the 
   * filter. 
   * Note that, currently we do not propagate filters over subqueries. For eg: if the query is of the type:
   * select ... FROM t1 JOIN (select ... t2) x where x.partition
   * we will not recognize that x.partition condition introduces a parition pruner on t2
   * 
   */
  @SuppressWarnings("nls")
  private void genPartitionPruners(QB qb) throws SemanticException {
    Map<String, Boolean> joinPartnPruner = new HashMap<String, Boolean>();
    QBParseInfo qbp = qb.getParseInfo();

    // Recursively prune subqueries
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genPartitionPruners(qbexpr);
    }

    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);

      PartitionPruner pruner = new PartitionPruner(alias, qb.getMetaData());
      // Pass each where clause to the pruner
      for(String clause: qbp.getClauseNames()) {

        CommonTree whexp = (CommonTree)qbp.getWhrForClause(clause);
        if (whexp != null) {
          pruner.addExpression((CommonTree)whexp.getChild(0));
        }
      }

      // Add the pruner to the list
      this.aliasToPruner.put(alias_id, pruner);
    }

    if (!qb.getTabAliases().isEmpty() && qb.getQbJoinTree() != null) {
      int pos = 0;
      for (String alias : qb.getQbJoinTree().getBaseSrc()) {
        if (alias != null) {
          String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
          PartitionPruner pruner = this.aliasToPruner.get(alias_id);
          if(pruner == null) {
            // this means that the alias is a subquery
            pos++;
            continue;
          }
          Vector<CommonTree> filters = qb.getQbJoinTree().getFilters().get(pos);
          for (CommonTree cond : filters) {
            pruner.addJoinOnExpression(cond);
            if (pruner.hasPartitionPredicate(cond))
              joinPartnPruner.put(alias_id, new Boolean(true));
          }
          if (qb.getQbJoinTree().getJoinSrc() != null) {
            filters = qb.getQbJoinTree().getFilters().get(0);
            for (CommonTree cond : filters) {
              pruner.addJoinOnExpression(cond);
              if (pruner.hasPartitionPredicate(cond))
                joinPartnPruner.put(alias_id, new Boolean(true));
            }
          }
        }
        pos++;
      }
    }

    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
      PartitionPruner pruner = this.aliasToPruner.get(alias_id);
      if (joinPartnPruner.get(alias_id) == null) {
        // Pass each where clause to the pruner
         for(String clause: qbp.getClauseNames()) {
          
           CommonTree whexp = (CommonTree)qbp.getWhrForClause(clause);
           if (pruner.getTable().isPartitioned() &&
               conf.getVar(HiveConf.ConfVars.HIVEPARTITIONPRUNER).equalsIgnoreCase("strict") &&
               (whexp == null || !pruner.hasPartitionPredicate((CommonTree)whexp.getChild(0)))) {
             throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE.getMsg(whexp != null ? whexp : qbp.getSelForClause(clause), 
                                                                                " for Alias " + alias + " Table " + pruner.getTable().getName()));
           }
         }
      }
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
  public void getMetaData(QB qb) throws SemanticException {
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
      String tblName = unescapeIdentifier(condn.getChild(0).getText().toLowerCase());
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

  /**
   * Parse the join condition. 
   * If the condition is a join condition, throw an error if it is not an equality. Otherwise, break it into left and 
   * right expressions and store in the join tree.
   * If the condition is a join filter, add it to the filter list of join tree.  The join condition can contains conditions
   * on both the left and tree trees and filters on either. Currently, we only support equi-joins, so we throw an error
   * if the condition involves both subtrees and is not a equality. Also, we only support AND i.e ORs are not supported 
   * currently as their semantics are not very clear, may lead to data explosion and there is no usecase.
   * @param joinTree  jointree to be populated
   * @param joinCond  join condition
   * @param leftSrc   left sources
   * @throws SemanticException
   */
  private void parseJoinCondition(QBJoinTree joinTree, CommonTree joinCond, Vector<String> leftSrc)
      throws SemanticException {

    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_3.getMsg(joinCond));
      
    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (CommonTree) joinCond
          .getChild(0), leftSrc);
      parseJoinCondition(joinTree, (CommonTree) joinCond
          .getChild(1), leftSrc);
      break;

    case HiveParser.EQUAL:
      CommonTree leftCondn = (CommonTree) joinCond.getChild(0);
      Vector<String> leftCondAl1 = new Vector<String>();
      Vector<String> leftCondAl2 = new Vector<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2);

      CommonTree rightCondn = (CommonTree) joinCond.getChild(1);
      Vector<String> rightCondAl1 = new Vector<String>();
      Vector<String> rightCondAl2 = new Vector<String>();
      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1, rightCondAl2);

      // is it a filter or a join condition
      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0)) ||
          ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0)))
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(joinCond));

      if (leftCondAl1.size() != 0) {
        if ((rightCondAl1.size() != 0) || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0)))
          joinTree.getFilters().get(0).add(joinCond);
        else if (rightCondAl2.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree, leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree, leftSrc);
        }
      }
      else if (leftCondAl2.size() != 0) {
        if ((rightCondAl2.size() != 0) || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0)))
          joinTree.getFilters().get(1).add(joinCond);
        else if (rightCondAl1.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree, leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree, leftSrc);
        }
      }
      else if (rightCondAl1.size() != 0)
        joinTree.getFilters().get(0).add(joinCond);
      else
        joinTree.getFilters().get(1).add(joinCond);

      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);
        
      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<Vector<String>> leftAlias = new ArrayList<Vector<String>>(joinCond.getChildCount() - childrenBegin);
      ArrayList<Vector<String>> rightAlias = new ArrayList<Vector<String>>(joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        Vector<String> left  = new Vector<String>();
        Vector<String> right = new Vector<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }
        
      for (int ci=childrenBegin; ci<joinCond.getChildCount(); ci++)
        parseJoinCondPopulateAlias(joinTree, (CommonTree)joinCond.getChild(ci), leftAlias.get(ci-childrenBegin), rightAlias.get(ci-childrenBegin));

      boolean leftAliasNull = true;
      for (Vector<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
    	}

      boolean rightAliasNull = true;
      for (Vector<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
    	}

      if (!leftAliasNull && !rightAliasNull)
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(joinCond));

      if (!leftAliasNull)
        joinTree.getFilters().get(0).add(joinCond);
      else
        joinTree.getFilters().get(1).add(joinCond);
        
      break;
    }
  }

  @SuppressWarnings("nls")
  private Operator<? extends Serializable> putOpInsertMap(Operator<? extends Serializable> op, RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    return op;
  }
  
  @SuppressWarnings("nls")
  private Operator genFilterPlan(String dest, QB qb,
      Operator input) throws SemanticException {

    CommonTree whereExpr = qb.getParseInfo().getWhrForClause(dest);
    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new filterDesc(genExprNodeDesc(qb.getMetaData(), (CommonTree)whereExpr.getChild(0), inputRR)),
          new RowSchema(inputRR.getColumnInfos()), input), inputRR);
 
    LOG.debug("Created Filter Plan for " + qb.getId() + ":" + dest + " row schema: " + inputRR.toString());
    return output;
  }

  /**
   * create a filter plan. The condition and the inputs are specified.
   * @param qb current query block
   * @param condn The condition to be resolved
   * @param input the input operator
   */
  @SuppressWarnings("nls")
  private Operator genFilterPlan(QB qb, CommonTree condn, Operator input) throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new filterDesc(genExprNodeDesc(qb.getMetaData(), condn, inputRR)),
          new RowSchema(inputRR.getColumnInfos()), input), inputRR);
 
    LOG.debug("Created Filter Plan for " + qb.getId() + " row schema: " + inputRR.toString());
    return output;
  }

  @SuppressWarnings("nls")
  private void genColList(String tabAlias, String alias, CommonTree sel,
    ArrayList<exprNodeDesc> col_list, RowResolver input, Integer pos,
    RowResolver output) throws SemanticException {

    // The table alias should exist
    if (tabAlias != null && !input.hasTableAlias(tabAlias))
      throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(sel));
    
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

  /**
   * If the user script command needs any modifications - do it here
   */
  private String getFixedCmd(String cmd) {
    SessionState ss = SessionState.get();
    if(ss == null)
      return cmd;

    // for local mode - replace any references to packaged files by name with 
    // the reference to the original file path
    if(ss.getConf().get("mapred.job.tracker", "local").equals("local")) {
      Set<String> files = ss.list_resource(SessionState.ResourceType.FILE, null);
      if((files != null) && !files.isEmpty()) {
        int end = cmd.indexOf(" ");
        String prog = (end == -1) ? cmd : cmd.substring(0, end);
        String args = (end == -1) ? "" :  cmd.substring(end, cmd.length());

        for(String oneFile: files) {
          Path p = new Path(oneFile);
          if(p.getName().equals(prog)) {
            cmd = oneFile + args;
            break;
          }
        }
      }
    }

    return cmd;
  }


  @SuppressWarnings("nls")
  private Operator genScriptPlan(CommonTree trfm, QB qb,
      Operator input) throws SemanticException {
    // If there is no "AS" clause, the output schema will be "key,value"
    ArrayList<String> outputColList = new ArrayList<String>();
    boolean defaultOutputColList = (trfm.getChildCount() < 3);
    if (defaultOutputColList) {
      outputColList.add("key");
      outputColList.add("value");
    } else {
      CommonTree collist = (CommonTree) trfm.getChild(2);
      int ccount = collist.getChildCount();
      for (int i=0; i < ccount; ++i) {
        outputColList.add(unescapeIdentifier(((CommonTree)collist.getChild(i)).getText()));
      }
    }
    
    RowResolver out_rwsch = new RowResolver();
    StringBuilder columns = new StringBuilder();
    for (int i = 0; i < outputColList.size(); ++i) {
      if (i != 0) {
        columns.append(",");
      }
      columns.append(outputColList.get(i));
      out_rwsch.put(
        qb.getParseInfo().getAlias(),
        outputColList.get(i),
        new ColumnInfo(outputColList.get(i), String.class)  // Script output is always a string
      );
    }

    Operator output = putOpInsertMap(OperatorFactory
            .getAndMakeChild(
                new scriptDesc(
                    getFixedCmd(stripQuotes(trfm.getChild(1).getText())),
                    PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.tabCode), columns.toString(), defaultOutputColList),
                    PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.tabCode), "")),
                    new RowSchema(
                        out_rwsch.getColumnInfos()), input), out_rwsch);

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
      return unescapeIdentifier(selExpr.getChild(1).getText()); 
    }

    CommonTree root = (CommonTree)selExpr.getChild(0);
    while (root.getType() == HiveParser.DOT || root.getType() == HiveParser.TOK_COLREF) {
      if (root.getType() == HiveParser.TOK_COLREF && root.getChildCount() == 1) {
        root = (CommonTree) root.getChild(0);
      }
      else {
        assert(root.getChildCount() == 2);
        root = (CommonTree) root.getChild(1);
      }
    }
    if (root.getType() == HiveParser.Identifier) {
      // Return zz for "xx.zz" and "xx.yy.zz"
      return unescapeIdentifier(root.getText());
    } else {
      // Return defaultName if selExpr is not a simple xx.yy.zz 
      return defaultName;
    }
  }
  
  @SuppressWarnings("nls")
  private Operator genSelectPlan(String dest, QB qb,
    Operator input) throws SemanticException {

    CommonTree selExprList = qb.getParseInfo().getSelForClause(dest);

    ArrayList<exprNodeDesc> col_list = new ArrayList<exprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    CommonTree trfm = null;
    String alias = qb.getParseInfo().getAlias();
    Integer pos = Integer.valueOf(0);
    RowResolver inputRR = opParseCtx.get(input).getRR();
    boolean selectStar = false;
    
    // Iterate over the selects
    for (int i = 0; i < selExprList.getChildCount(); ++i) {

      // list of the columns
      CommonTree selExpr = (CommonTree) selExprList.getChild(i);
      String colAlias = getColAlias(selExpr, "_C" + i);
      CommonTree sel = (CommonTree)selExpr.getChild(0);
      
      if (sel.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
        String tabAlias = null;
        if (sel.getChildCount() == 1)
          tabAlias = unescapeIdentifier(sel.getChild(0).getText().toLowerCase());
        genColList(tabAlias, alias, sel, col_list, inputRR, pos, out_rwsch);
        selectStar = true;
      } else if (sel.getToken().getType() == HiveParser.TOK_TRANSFORM) {
        if (i > 0) {
          throw new SemanticException(ErrorMsg.INVALID_TRANSFORM.getMsg(sel));
        }
        trfm = sel;
        CommonTree cols = (CommonTree) trfm.getChild(0);
        for (int j = 0; j < cols.getChildCount(); ++j) {
          CommonTree expr = (CommonTree) cols.getChild(j);
          if (expr.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
            String tabAlias = null;
            if (sel.getChildCount() == 1)
              tabAlias = unescapeIdentifier(sel.getChild(0).getText().toLowerCase());

            genColList(tabAlias, alias, expr, col_list, inputRR, pos, out_rwsch);
            selectStar = true;
          } else {
            exprNodeDesc exp = genExprNodeDesc(qb.getMetaData(), expr, inputRR);
            col_list.add(exp);
            if (!StringUtils.isEmpty(alias) &&
                (out_rwsch.get(alias, colAlias) != null)) {
              throw new SemanticException(ErrorMsg.AMBIGOUS_COLUMN.getMsg(expr.getChild(1)));
            }

            out_rwsch.put(alias, unescapeIdentifier(expr.getText()),
                          new ColumnInfo((Integer.valueOf(pos)).toString(),
                                         exp.getTypeInfo()));
          }
        }
      } else {
        // Case when this is an expression
        exprNodeDesc exp = genExprNodeDesc(qb.getMetaData(), sel, inputRR);
        col_list.add(exp);
        if (!StringUtils.isEmpty(alias) &&
            (out_rwsch.get(alias, colAlias) != null)) {
          throw new SemanticException(ErrorMsg.AMBIGOUS_COLUMN.getMsg(sel.getChild(1)));
        }
        // Since the as clause is lacking we just use the text representation
        // of the expression as the column name
        out_rwsch.put(alias, colAlias,
                      new ColumnInfo((Integer.valueOf(pos)).toString(),
                                     exp.getTypeInfo()));
      }
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    for (int i=0; i<col_list.size(); i++) {
      if (col_list.get(i) instanceof exprNodeNullDesc) {
        col_list.set(i, new exprNodeConstantDesc(String.class, null));
      }
    }
    
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(col_list, (selExprList.getChildCount() == 1) && selectStar), new RowSchema(out_rwsch.getColumnInfos()),
        input), out_rwsch);

    if (trfm != null) {
      output = genScriptPlan(trfm, qb, output);
    }

    LOG.debug("Created Select Plan for clause: " + dest + " row schema: " + out_rwsch.toString());

    return output;
  }

  /**
   * Class to store UDAF related information.
   */
  static class UDAFInfo {
    ArrayList<exprNodeDesc> convertedParameters;
    Method aggregateMethod;
    Method evaluateMethod;
  }

  /**
   * Returns the UDAFInfo struct for the aggregation
   * @param aggName  The name of the UDAF.
   * @param mode     The mode of the aggregation. This affects the evaluate method.
   * @param aggClasses  The classes of the parameters to the UDAF. 
   * @param aggParameters  The actual exprNodeDesc of the parameters.
   * @param aggTree   The CommonTree node of the UDAF in the query.
   * @return UDAFInfo
   * @throws SemanticException when the UDAF is not found or has problems.
   */
  UDAFInfo getUDAFInfo(String aggName, groupByDesc.Mode mode, ArrayList<Class<?>> aggClasses,
      ArrayList<exprNodeDesc> aggParameters, CommonTree aggTree) throws SemanticException {
    UDAFInfo r = new UDAFInfo();
    r.aggregateMethod = FunctionRegistry.getUDAFMethod(aggName, aggClasses);
    if (null == r.aggregateMethod) {
      String reason = "Looking for UDAF \"" + aggName + "\" with parameters " + aggClasses;
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg((CommonTree)aggTree.getChild(0), reason));
    }

    r.convertedParameters = convertParameters(r.aggregateMethod, aggParameters);
    
    r.evaluateMethod = FunctionRegistry.getUDAFEvaluateMethod(aggName, mode);
    if (r.evaluateMethod == null) {
      String reason = "UDAF \"" + aggName + "\" does not have evaluate()/evaluatePartial() methods.";
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg((CommonTree)aggTree.getChild(0), reason)); 
    }
    
    return r;
  }
  
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator(
        QBParseInfo parseInfo, String dest, Operator reduceSinkOperatorInfo,
        groupByDesc.Mode mode)
    throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx.get(reduceSinkOperatorInfo).getRR();
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

      UDAFInfo udaf = getUDAFInfo(aggName, mode, aggClasses, aggParameters, value);
      
      aggregations.add(new aggregationDesc(aggClass, udaf.convertedParameters,
          value.getToken().getType() == HiveParser.TOK_FUNCTIONDI));
      groupByOutputRowResolver.put("",value.toStringTree(),
                                   new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() -1).toString(),
                                       udaf.evaluateMethod.getReturnType()));
    }

    return 
      putOpInsertMap(OperatorFactory.getAndMakeChild(new groupByDesc(mode, groupByKeys, aggregations),
                                                     new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                                     reduceSinkOperatorInfo),
        groupByOutputRowResolver
    );
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator1(
        QBParseInfo parseInfo, String dest, Operator reduceSinkOperatorInfo,
        groupByDesc.Mode mode)
    throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx.get(reduceSinkOperatorInfo).getRR();
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

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      CommonTree value = parseInfo.getDistinctFuncExprForClause(dest);
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

      UDAFInfo udaf = getUDAFInfo(aggName, mode, aggClasses, aggParameters, value);
      
      aggregations.add(new aggregationDesc(aggClass, udaf.convertedParameters, true));
      groupByOutputRowResolver.put("",value.toStringTree(),
                                   new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() -1).toString(),
                                       udaf.evaluateMethod.getReturnType()));
    }

    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      CommonTree value = entry.getValue();
      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI)
        continue;

      String aggName = value.getChild(0).getText();
      Class<? extends UDAF> aggClass = FunctionRegistry.getUDAF(aggName);
      assert (aggClass != null);
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      String text = entry.getKey();
      ColumnInfo paraExprInfo = groupByInputRowResolver.get("",text);
      if (paraExprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
      }
      String paraExpression = paraExprInfo.getInternalName();
      assert(paraExpression != null);
      aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), paraExpression));
      aggregations.add(new aggregationDesc(aggClass, aggParameters, ((mode == groupByDesc.Mode.FINAL) ? false : (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI))));
      groupByOutputRowResolver.put("", value.toStringTree(),
                                    new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() - 1).toString(),
                                                   paraExprInfo.getType()));
    }

    return putOpInsertMap(
        OperatorFactory.getAndMakeChild(new groupByDesc(mode, groupByKeys, aggregations),
                                        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                        reduceSinkOperatorInfo),
        groupByOutputRowResolver);
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapGroupByOperator(QB qb, String dest, Operator inputOperatorInfo, 
                                                    groupByDesc.Mode mode) throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo).getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      exprNodeDesc grpByExprNode = genExprNodeDesc(qb.getMetaData(), grpbyExpr, groupByInputRowResolver);

      groupByKeys.add(grpByExprNode);
      String field = (Integer.valueOf(i)).toString();
      groupByOutputRowResolver.put("",grpbyExpr.toStringTree(),
                                   new ColumnInfo(field, grpByExprNode.getTypeInfo()));
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      CommonTree value = parseInfo.getDistinctFuncExprForClause(dest);
      int numDistn=0;
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        CommonTree parameter = (CommonTree) value.getChild(i);
        String text = parameter.toStringTree();
        if (groupByOutputRowResolver.get("",text) == null) {
          exprNodeDesc distExprNode = genExprNodeDesc(qb.getMetaData(), parameter, groupByInputRowResolver);
          groupByKeys.add(distExprNode);
          numDistn++;
          String field = (Integer.valueOf(grpByExprs.size() + numDistn -1)).toString();
          groupByOutputRowResolver.put("", text, new ColumnInfo(field, distExprNode.getTypeInfo()));
        }
      }
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
        CommonTree paraExpr = (CommonTree)value.getChild(i);
        exprNodeDesc paraExprNode = genExprNodeDesc(qb.getMetaData(), paraExpr, groupByInputRowResolver);

        aggParameters.add(paraExprNode);
        aggClasses.add(paraExprNode.getTypeInfo().getPrimitiveClass());
      }

      UDAFInfo udaf = getUDAFInfo(aggName, mode, aggClasses, aggParameters, value);
      
      aggregations.add(new aggregationDesc(aggClass, udaf.convertedParameters,
                                           value.getToken().getType() == HiveParser.TOK_FUNCTIONDI));
      groupByOutputRowResolver.put("",value.toStringTree(),
                                   new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() -1).toString(),
                                       udaf.evaluateMethod.getReturnType()));
    }

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(new groupByDesc(mode, groupByKeys, aggregations),
                                      new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                      inputOperatorInfo),
      groupByOutputRowResolver);
  }

  private ArrayList<exprNodeDesc> convertParameters(Method m, ArrayList<exprNodeDesc> aggParameters) {
    
    ArrayList<exprNodeDesc> newParameters = new ArrayList<exprNodeDesc>();
    Class<?>[] pTypes = m.getParameterTypes();

    // 0 is the function name
    for (int i = 0; i < aggParameters.size(); i++) {
      exprNodeDesc desc = aggParameters.get(i);
      Class<?> pType = ObjectInspectorUtils.generalizePrimitive(pTypes[i]);
      if (desc instanceof exprNodeNullDesc) {
        exprNodeConstantDesc newCh = new exprNodeConstantDesc(TypeInfoFactory.getPrimitiveTypeInfo(pType), null);
        newParameters.add(newCh);
      } else if (pType.isAssignableFrom(desc.getTypeInfo().getPrimitiveClass())) {
        // no type conversion needed
        newParameters.add(desc);
      } else {
        // must be implicit type conversion
        Class<?> from = desc.getTypeInfo().getPrimitiveClass();
        Class<?> to = pType;
        assert(FunctionRegistry.implicitConvertable(from, to));
        Method conv = FunctionRegistry.getUDFMethod(to.getName(), true, from);
        assert(conv != null);
        Class<? extends UDF> c = FunctionRegistry.getUDFClass(to.getName());
        assert(c != null);
        
        // get the conversion method
        ArrayList<exprNodeDesc> conversionArg = new ArrayList<exprNodeDesc>(1);
        conversionArg.add(desc);
        newParameters.add(new exprNodeFuncDesc(TypeInfoFactory.getPrimitiveTypeInfo(pType),
                                               c, conv, conversionArg));
      }
    }

    return newParameters;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator(QBParseInfo parseInfo,
      String dest, Operator inputOperatorInfo)
      throws SemanticException {
    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo).getRR();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    // Pre-compute group-by keys and store in reduceKeys
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();

      if (reduceSinkOutputRowResolver.get("", text) == null) {
        ColumnInfo exprInfo = reduceSinkInputRowResolver.get("", text);
        reduceKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo.getInternalName()));
        reduceSinkOutputRowResolver.put("", text,
                                        new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                                                       exprInfo.getType()));
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
          ColumnInfo exprInfo = reduceSinkInputRowResolver.get("", text);
          reduceKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo.getInternalName()));
          reduceSinkOutputRowResolver.put("", text,
                                          new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                                                         exprInfo.getType()));
        }
      }
    }

    // Put partial aggregation results in reduceValues
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    int inputField = reduceKeys.size();

    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {

      TypeInfo type = reduceSinkInputRowResolver.getColumnInfos().get(inputField).getType(); 
      reduceValues.add(new exprNodeColumnDesc(
          type, (Integer.valueOf(inputField)).toString()));
      inputField++;
      reduceSinkOutputRowResolver.put("", ((CommonTree)entry.getValue()).toStringTree(),
                                      new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + (Integer.valueOf(reduceValues.size()-1)).toString(),
                                                     type));
    }

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, -1,
                                    (parseInfo.getDistinctFuncExprForClause(dest) == null ? -1 : Integer.MAX_VALUE), -1, false),
        new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
        inputOperatorInfo),
      reduceSinkOutputRowResolver);
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator(QB qb,
      String dest, Operator inputOperatorInfo, int numPartitionFields) throws SemanticException {
    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo).getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      reduceKeys.add(genExprNodeDesc(qb.getMetaData(), grpbyExpr, reduceSinkInputRowResolver));
      String text = grpbyExpr.toStringTree();
      if (reduceSinkOutputRowResolver.get("", text) == null) {
        reduceSinkOutputRowResolver.put("", text,
                                        new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                                            reduceKeys.get(reduceKeys.size()-1).getTypeInfo()));
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
          reduceKeys.add(genExprNodeDesc(qb.getMetaData(), parameter, reduceSinkInputRowResolver));
          reduceSinkOutputRowResolver.put("", text,
                                          new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + Integer.valueOf(reduceKeys.size() - 1).toString(),
                                              reduceKeys.get(reduceKeys.size()-1).getTypeInfo()));
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
          reduceValues.add(genExprNodeDesc(qb.getMetaData(), parameter, reduceSinkInputRowResolver));
          reduceSinkOutputRowResolver.put("", text,
                                          new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + Integer.valueOf(reduceValues.size() - 1).toString(),
                                              reduceValues.get(reduceValues.size()-1).getTypeInfo()));
        }
      }
    }

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, -1, numPartitionFields,
                                                                  -1, false),
                                        new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
                                        inputOperatorInfo),
        reduceSinkOutputRowResolver
    );
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator2MR(
      QBParseInfo parseInfo, String dest, Operator groupByOperatorInfo, int numPartitionFields)
    throws SemanticException {
    RowResolver reduceSinkInputRowResolver2 = opParseCtx.get(groupByOperatorInfo).getRR();
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    // Get group-by keys and store in reduceKeys
    List<CommonTree> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      CommonTree grpbyExpr = grpByExprs.get(i);
      String field = (Integer.valueOf(i)).toString();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.get("", grpbyExpr.toStringTree()).getType();
      reduceKeys.add(new exprNodeColumnDesc(typeInfo, field));
      reduceSinkOutputRowResolver2.put("", grpbyExpr.toStringTree(),
                                       new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + field,
                                           typeInfo));
    }
    // Get partial aggregation results and store in reduceValues
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    int inputField = reduceKeys.size();
    HashMap<String, CommonTree> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, CommonTree> entry : aggregationTrees.entrySet()) {
      String field = (Integer.valueOf(inputField)).toString();
      CommonTree t = entry.getValue();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.get("", t.toStringTree()).getType();
      reduceValues.add(new exprNodeColumnDesc(typeInfo, field));
      inputField++;
      reduceSinkOutputRowResolver2.put("", t.toStringTree(),
                                       new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + (Integer.valueOf(reduceValues.size()-1)).toString(),
                                           typeInfo));
    }

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, -1, 
                                                                  numPartitionFields, -1, true),
                                        new RowSchema(reduceSinkOutputRowResolver2.getColumnInfos()),
                                        groupByOperatorInfo),
        reduceSinkOutputRowResolver2
    );
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator2MR(
    QBParseInfo parseInfo, String dest, Operator reduceSinkOperatorInfo2, groupByDesc.Mode mode)
    throws SemanticException {
    RowResolver groupByInputRowResolver2 = opParseCtx.get(reduceSinkOperatorInfo2).getRR();
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
      aggregations.add(new aggregationDesc(aggClass, aggParameters, ((mode == groupByDesc.Mode.FINAL) ? false : (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI))));
      groupByOutputRowResolver2.put("", value.toStringTree(),
                                    new ColumnInfo(Integer.valueOf(groupByKeys.size() + aggregations.size() - 1).toString(),
                                                   paraExprInfo.getType()));
    }

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(new groupByDesc(mode, groupByKeys, aggregations),
                                      new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
                                      reduceSinkOperatorInfo2),
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
  private Operator genGroupByPlan1MR(String dest, QB qb,
      Operator input) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 1. Generate ReduceSinkOperator
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(
        qb, dest, input, getGroupByForClause(parseInfo, dest).size());


    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
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
  private Operator genGroupByPlan2MR(String dest, QB qb,
      Operator input) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 1. Generate ReduceSinkOperator
    // There is a special case when we want the rows to be randomly distributed to  
    // reducers for load balancing problem.  That happens when there is no DISTINCT
    // operator.  We set the numPartitionColumns to -1 for this purpose.  This is 
    // captured by WritableComparableHiveObject.hashCode() function. 
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(
      qb, dest, input, (parseInfo.getDistinctFuncExprForClause(dest) == null ? -1
            : Integer.MAX_VALUE));

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIAL1);

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo,
        getGroupByForClause(parseInfo, dest).size());

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = 
      genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL);

    return groupByOperatorInfo2;
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) {
    List<CommonTree> grpByExprs = getGroupByForClause(qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty())
      return false;

    if (qb.getParseInfo().getDistinctFuncExprForClause(dest) != null)
      return false;

    return true;
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs. First perform a map
   * side partial aggregation (to reduce the amount of data). Then spray by
   * the distinct key (or a random number) in hope of getting a uniform 
   * distribution, and compute partial aggregates grouped by that distinct key.
   * Evaluate partial aggregates first, followed by actual aggregates.
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan4MR(String dest, QB qb, 
    Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Operator groupByOperatorInfo = genGroupByPlanMapGroupByOperator(qb,
      dest, inputOperatorInfo, groupByDesc.Mode.HASH);

    // ////// Generate ReduceSink Operator
    Operator reduceSinkOperatorInfo = 
      genGroupByPlanReduceSinkOperator(parseInfo, dest, groupByOperatorInfo);

    // Optimize the scenario when there are no grouping keys and no distinct - 2 map-reduce jobs are not needed
    if (!optimizeMapAggrGroupBy(dest, qb)) {
      // ////// Generate GroupbyOperator for a partial aggregation
      Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator1(parseInfo, dest, reduceSinkOperatorInfo, 
                                                                         groupByDesc.Mode.PARTIAL2);
      
      // //////  Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(parseInfo, dest, groupByOperatorInfo2,
                                                                                 getGroupByForClause(parseInfo, dest).size());

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL);
    }
    else
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo, groupByDesc.Mode.FINAL);
  }

  @SuppressWarnings("nls")
  private Operator genConversionOps(String dest, QB qb,
      Operator input) throws SemanticException {

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
  private Operator genFileSinkPlan(String dest, QB qb,
      Operator input) throws SemanticException {

  	RowResolver inputRR = opParseCtx.get(input).getRR();
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
        dest_path = qb.getMetaData().getDestFileForAlias(dest);
        String cols = new String();
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
        table_desc = PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.ctrlaCode),
            cols);
        break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + dest_type);
    }

    input = genConversionSelectOperator(dest, qb, input, table_desc);

    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new fileSinkDesc(queryTmpdir, table_desc),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    LOG.debug("Created FileSink Plan for clause: " + dest + "dest_path: "
        + dest_path + " row schema: "
        + inputRR.toString());
    return output;
  }

  /**
   * Generate the conversion SelectOperator that converts the columns into 
   * the types that are expected by the table_desc.
   */
  Operator genConversionSelectOperator(String dest, QB qb,
      Operator input, tableDesc table_desc) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass().newInstance();
      deserializer.initialize(null, table_desc.getProperties());
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    
    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    Vector<ColumnInfo> rowFields = opParseCtx.get(input).getRR().getColumnInfos();
    if (tableFields.size() != rowFields.size()) {
      String reason = "Table " + dest + " has " + tableFields.size() + " columns but query has "
          + rowFields + ".";
      throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
          qb.getParseInfo().getDestForClause(dest), reason));
    }
    
    // Check column types
    boolean converted = false;
    int columnNumber = tableFields.size();
    ArrayList<exprNodeDesc> expressions = new ArrayList<exprNodeDesc>(columnNumber);
    // MetadataTypedColumnsetSerDe does not need type conversions because it does
    // the conversion to String by itself.
    if (! table_desc.getDeserializerClass().equals(MetadataTypedColumnsetSerDe.class)) { 
      for (int i=0; i<columnNumber; i++) {
        ObjectInspector tableFieldOI = tableFields.get(i).getFieldObjectInspector();
        TypeInfo tableFieldTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(tableFieldOI);
        TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
        exprNodeDesc column = new exprNodeColumnDesc(rowFieldTypeInfo, Integer.valueOf(i).toString());
        if (! tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
          // need to do some conversions here
          converted = true;
          if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
            // cannot convert to complex types
            column = null; 
          } else {
            column = getFuncExprNodeDesc(tableFieldTypeInfo.getPrimitiveClass().getName(), column);
          }
          if (column == null) {
            String reason = "Cannot convert column " + i + " from " + rowFieldTypeInfo + " to " 
                + tableFieldTypeInfo + ".";
            throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
                qb.getParseInfo().getDestForClause(dest), reason));
          }
        } else {
          expressions.add(column);
        }
      }
    }
    
    if (converted) {
      // add the select operator
      RowResolver rowResolver = new RowResolver();
      for (int i=0; i<expressions.size(); i++) {
        String name = Integer.valueOf(i).toString();
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i).getTypeInfo()));
      }
      Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(expressions), new RowSchema(rowResolver.getColumnInfos()), input), rowResolver);

      return output;
    } else {
      // not converted
      return input;
    }
  }

  @SuppressWarnings("nls")
  private Operator genLimitPlan(String dest, QB qb, Operator input, int limit) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase. A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields

  	RowResolver inputRR = opParseCtx.get(input).getRR();
    Operator limitMap = 
      putOpInsertMap(OperatorFactory.getAndMakeChild(
                       new limitDesc(limit), new RowSchema(inputRR.getColumnInfos()), input), 
                       inputRR);
      

    LOG.debug("Created LimitOperator Plan for clause: " + dest + " row schema: "
        + inputRR.toString());

    return limitMap;
  }

  @SuppressWarnings("nls")
  private Operator genLimitMapRedPlan(String dest, QB qb, Operator input, int limit, boolean isOuterQuery) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase. A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields
    Operator curr = genLimitPlan(dest, qb, input, limit);

    if (isOuterQuery)
      return curr;

    // Create a reduceSink operator followed by another limit
    curr = genReduceSinkPlan(dest, qb, curr, 1);
    return genLimitPlan(dest, qb, curr, limit);
  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlan(String dest, QB qb,
                                     Operator input, int numReducers) throws SemanticException {

    // First generate the expression for the key
    // The cluster by clause has the aliases for the keys
    ArrayList<exprNodeDesc> keyCols = new ArrayList<exprNodeDesc>();
    RowResolver inputRR = opParseCtx.get(input).getRR();
    
    CommonTree clby = qb.getParseInfo().getClusterByForClause(dest);
    if (clby != null) {
      int ccount = clby.getChildCount();
      for(int i=0; i<ccount; ++i) {
        CommonTree cl = (CommonTree)clby.getChild(i);
        ColumnInfo colInfo = inputRR.get(qb.getParseInfo().getAlias(),
                                         unescapeIdentifier(cl.getText()));
        if (colInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(cl));
        }
        
        keyCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName()));
      }
    }

    ArrayList<exprNodeDesc> valueCols = new ArrayList<exprNodeDesc>();

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    for(ColumnInfo colInfo: inputRR.getColumnInfos()) {
      valueCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName()));
    }

    Operator interim = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(keyCols, valueCols, -1, keyCols.size(), numReducers, false),
        new RowSchema(inputRR.getColumnInfos()),
        input), inputRR);

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for(ColumnInfo colInfo: interim_rwsch.getColumnInfos()) {
      String [] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1],
                    new ColumnInfo(pos.toString(), colInfo.getType()));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(String.class, Utilities.ReduceField.VALUE.toString())),
        new RowSchema(out_rwsch.getColumnInfos()),
        interim), out_rwsch);

    LOG.debug("Created ReduceSink Plan for clause: " + dest + " row schema: "
        + out_rwsch.toString());
    return output;
  }

  private Operator genJoinOperatorChildren(QBJoinTree join, Operator left, Operator[] right) 
    throws SemanticException {
    RowResolver outputRS = new RowResolver();
    // all children are base classes
    Operator<?>[] rightOps = new Operator[right.length];
    int pos = 0;
    int outputPos = 0;

    HashMap<Byte, ArrayList<exprNodeDesc>> exprMap = new HashMap<Byte, ArrayList<exprNodeDesc>>();

    for (Operator input : right)
    {
      ArrayList<exprNodeDesc> keyDesc = new ArrayList<exprNodeDesc>();
      if (input == null)
        input = left;
      Byte tag = Byte.valueOf((byte)(((reduceSinkDesc)(input.getConf())).getTag()));
      RowResolver inputRS = opParseCtx.get(input).getRR();
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
      rightOps[pos++] = input;
    }

    org.apache.hadoop.hive.ql.plan.joinCond[] joinCondns = new org.apache.hadoop.hive.ql.plan.joinCond[join.getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      joinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new org.apache.hadoop.hive.ql.plan.joinCond(condn);
    }

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(new joinDesc(exprMap, joinCondns),
                                      new RowSchema(outputRS.getColumnInfos()), rightOps), outputRS);
  }

  @SuppressWarnings("nls")
  private Operator genJoinReduceSinkChild(QB qb, QBJoinTree joinTree,
      Operator child, String srcName, int pos) throws SemanticException {
    RowResolver inputRS = opParseCtx.get(child).getRR();
    RowResolver outputRS = new RowResolver();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    // Compute join keys and store in reduceKeys
    Vector<CommonTree> exprs = joinTree.getExpressions().get(pos);
    for (int i = 0; i < exprs.size(); i++) {
      CommonTree expr = exprs.get(i);
      reduceKeys.add(genExprNodeDesc(qb.getMetaData(), expr, inputRS));
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

    return putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, joinTree.getNextTag(), reduceKeys.size(), -1, false), 
        new RowSchema(outputRS.getColumnInfos()),
        child), outputRS);
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
      HashMap<String, Operator> map) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    Operator joinSrcOp = null;
    if (leftChild != null)
    {
      Operator joinOp = genJoinOperator(qb, leftChild, map);
      Vector<CommonTree> filter = joinTree.getFilters().get(0);
      for (CommonTree cond: filter) 
        joinOp = genFilterPlan(qb, cond, joinOp);
      
      joinSrcOp = genJoinReduceSinkChild(qb, joinTree, joinOp, null, 0);
    }

    Operator[] srcOps = new Operator[joinTree.getBaseSrc().length];
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
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

  private void genJoinOperatorTypeCheck(Operator left, Operator[] right) throws SemanticException {
    // keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key list 
    ArrayList<ArrayList<exprNodeDesc>> keys = new ArrayList<ArrayList<exprNodeDesc>>();
    int keyLength = 0;
    for (int i=0; i<right.length; i++) {
      Operator oi = (i==0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator)(oi)).getConf();
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
    // regenerate keySerializationInfo because the ReduceSinkOperator's 
    // output key types might have changed.
    for (int i=0; i<right.length; i++) {
      Operator oi = (i==0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator)(oi)).getConf();
      now.setKeySerializeInfo(PlanUtils.getBinarySortableTableDesc(
          PlanUtils.getFieldSchemasFromColumnList(now.getKeyCols(), "joinkey")));
    }
  }
  
  private Operator genJoinPlan(QB qb, HashMap<String, Operator> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    Operator joinOp = genJoinOperator(qb, joinTree, map);
    return joinOp;
  }

  /**
   * Extract the filters from the join condition and push them on top of the source operators. This procedure 
   * traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree, HashMap<String, Operator> map) throws SemanticException {
    Vector<Vector<CommonTree>> filters = joinTree.getFilters();
    if (joinTree.getJoinSrc() != null)
      pushJoinFilters(qb, joinTree.getJoinSrc(), map);

    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        Vector<CommonTree> filter = filters.get(pos);
        for (CommonTree cond: filter) 
          srcOp = genFilterPlan(qb, cond, srcOp);
        map.put(src, srcOp);
      }
      pos++;
    }
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
      String table_name = unescapeIdentifier(left.getChild(0).getText());
      String alias = left.getChildCount() == 1 ? table_name : 
        unescapeIdentifier(left.getChild(1).getText().toLowerCase());
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
      String table_name = unescapeIdentifier(right.getChild(0).getText());
      String alias = right.getChildCount() == 1 ? table_name : 
        unescapeIdentifier(right.getChild(1).getText().toLowerCase());
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

    Vector<Vector<CommonTree>> filters = new Vector<Vector<CommonTree>>();
    filters.add(new Vector<CommonTree>());
    filters.add(new Vector<CommonTree>());
    joinTree.setFilters(filters);

    CommonTree joinCond = (CommonTree) joinParseTree.getChild(2);
    assert joinCond != null;
    Vector<String> leftSrc = new Vector<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc);
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

    Vector<Vector<CommonTree>> filter = target.getFilters();
    for (int i = 0; i < nodeRightAliases.length; i++)
      filter.add(node.getFilters().get(i + 1));

    if (node.getFilters().get(0).size() != 0) {
      Vector<CommonTree> filterPos = filter.get(pos);
      filterPos.addAll(node.getFilters().get(0));
    }

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
  private Operator genBodyPlan(QB qb, Operator input)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // Go over all the destination tables
    Operator curr = null;
    for (String dest : ks) {
      curr = input;      

      if (qbp.getWhrForClause(dest) != null) {
        curr = genFilterPlan(dest, qb, curr);
      }

      if (qbp.getAggregationExprsForClause(dest).size() != 0
          || getGroupByForClause(qbp, dest).size() > 0)
      {
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE).equalsIgnoreCase("true"))
          curr = genGroupByPlan4MR(dest, qb, curr);
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
  private Operator genUnionPlan(String unionalias, String leftalias,
      Operator leftOp, String rightalias, Operator rightOp)
      throws SemanticException {

    RowResolver leftRR = opParseCtx.get(leftOp).getRR();
    RowResolver rightRR = opParseCtx.get(rightOp).getRR();
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
    rightOp.setChildOperators(child);
    leftOp.setChildOperators(child);
    List<Operator<? extends Serializable>> parent = new ArrayList<Operator<? extends Serializable>>();
    parent.add(leftOp);
    parent.add(rightOp);
    unionforward.setParentOperators(parent);
    // create operator info list to return
    return putOpInsertMap(unionforward, unionoutRR);
  }

  /**
   * Generates the sampling predicate from the TABLESAMPLE clause information. This function uses the 
   * bucket column list to decide the expression inputs to the predicate hash function in case useBucketCols
   * is set to true, otherwise the expression list stored in the TableSample is used. The bucket columns of 
   * the table are used to generate this predicate in case no expressions are provided on the TABLESAMPLE
   * clause and the table has clustering columns defined in it's metadata.
   * The predicate created has the following structure:
   * 
   *     ((default_sample_hashfn(expressions) & Integer.MAX_VALUE) % denominator) == numerator
   * 
   * @param ts TABLESAMPLE clause information
   * @param bucketCols The clustering columns of the table
   * @param useBucketCols Flag to indicate whether the bucketCols should be used as input to the hash
   *                      function
   * @param alias The alias used for the table in the row resolver
   * @param rwsch The row resolver used to resolve column references
   * @param qbm The metadata information for the query block which is used to resolve unaliased columns
   * @return exprNodeDesc
   * @exception SemanticException
   */
  private exprNodeDesc genSamplePredicate(TableSample ts, List<String> bucketCols,
		                                  boolean useBucketCols, String alias,
		                                  RowResolver rwsch, QBMetaData qbm) 
    throws SemanticException {
	  
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
    if (useBucketCols) {
      for (String col : bucketCols) {
    	 ColumnInfo ci = rwsch.get(alias, col);
         // TODO: change type to the one in the table schema
    	 args.add(new exprNodeColumnDesc(ci.getType().getPrimitiveClass(), col));
      }
    }
    else {
      for(CommonTree expr: ts.getExprs()) {
    	  args.add(genExprNodeDesc(qbm, expr, rwsch));
      }
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
  private Operator genTablePlan(String alias, QB qb) throws SemanticException {

    String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
    Table tab = qb.getMetaData().getSrcForAlias(alias);
    RowResolver rwsch;
    
    // is the table already present
    Operator<? extends Serializable> top = this.topOps.get(alias_id);
    Operator<? extends Serializable> dummySel = this.topSelOps.get(alias_id);
    if (dummySel != null)
      top = dummySel;
   
    if (top == null) {    
      rwsch = new RowResolver();
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
      top = putOpInsertMap(OperatorFactory.get(forwardDesc.class, new RowSchema(rwsch.getColumnInfos())), rwsch);

      // Add this to the list of top operators - we always start from a table scan
      this.topOps.put(alias_id, top);
    }
    else {
    	rwsch = opParseCtx.get(top).getRR();
      top.setChildOperators(null);
    }
    
    // check if this table is sampled and needs more than input pruning
		Operator<? extends Serializable> tableOp = top;
		TableSample ts = qb.getParseInfo().getTabSample(alias);
    if (ts != null) {
      int num = ts.getNumerator();
      int den = ts.getDenominator();
      ArrayList<CommonTree> sampleExprs = ts.getExprs();
      
      // TODO: Do the type checking of the expressions
      List<String> tabBucketCols = tab.getBucketCols();
      int numBuckets = tab.getNumBuckets();

      // If there are no sample cols and no bucket cols then throw an error
      if (tabBucketCols.size() == 0 && sampleExprs.size() == 0) {
          throw new SemanticException(ErrorMsg.NON_BUCKETED_TABLE.getMsg() + " " + tab.getName());
      }

      // check if a predicate is needed
      // predicate is needed if either input pruning is not enough
      // or if input pruning is not possible
      
      // check if the sample columns are the same as the table bucket columns
      boolean colsEqual = true;
      if ( (sampleExprs.size() != tabBucketCols.size()) && (sampleExprs.size() != 0) ) {
        colsEqual = false;
      }
      
      for (int i = 0; i < sampleExprs.size() && colsEqual; i++) {
        boolean colFound = false;
        for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
          if (sampleExprs.get(i).getToken().getType() != HiveParser.TOK_COLREF) {
        	  break;
          }
          
          if (sampleExprs.get(i).getChildCount() != 1) {
            throw new SemanticException(ErrorMsg.TABLE_ALIAS_NOT_ALLOWED.getMsg());
          }

          if (((CommonTree)sampleExprs.get(i).getChild(0)).getText().equalsIgnoreCase(tabBucketCols.get(j))) {
            colFound = true;
          }
        }
        colsEqual = (colsEqual && colFound);
      }
      
      // Check if input can be pruned
      ts.setInputPruning((sampleExprs == null || sampleExprs.size() == 0 || colsEqual));
      
      // check if input pruning is enough     
      if ((sampleExprs == null || sampleExprs.size() == 0 || colsEqual)
          && (num == den || den <= numBuckets && numBuckets % den == 0)) { 
        // input pruning is enough; no need for filter
        LOG.info("No need for sample filter");
      } 
      else {
        // need to add filter
        // create tableOp to be filterDesc and set as child to 'top'
        LOG.info("Need sample filter");
        exprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols, colsEqual, alias, rwsch, qb.getMetaData());
        tableOp = OperatorFactory.getAndMakeChild(
            new filterDesc(samplePredicate), 
            top);
      }
    }
    
    Operator output = putOpInsertMap(tableOp, rwsch);

    LOG.debug("Created Table Plan for " + alias + " " + tableOp.toString());

    return output;
  }

  private Operator genPlan(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      return genPlan(qbexpr.getQB());
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
      Operator qbexpr1Ops = genPlan(qbexpr.getQBExpr1());
      Operator qbexpr2Ops = genPlan(qbexpr.getQBExpr2());

      return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(),
          qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    return null;
  }

  @SuppressWarnings("nls")
  public Operator genPlan(QB qb) throws SemanticException {

    // First generate all the opInfos for the elements in the from clause
    HashMap<String, Operator> aliasToOpInfo = new HashMap<String, Operator>();

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

    Operator srcOpInfo = null;

    // process join
    if (qb.getParseInfo().getJoinExpr() != null) {
      CommonTree joinExpr = qb.getParseInfo().getJoinExpr();
      QBJoinTree joinTree = genJoinTree(joinExpr);
      qb.setQbJoinTree(joinTree);
      mergeJoinTree(qb);

      // if any filters are present in the join tree, push them on top of the table
      pushJoinFilters(qb, qb.getQbJoinTree(), aliasToOpInfo);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    }
    else
      // Now if there are more than 1 sources then we have a join case
      // later we can extend this to the union all case as well
      srcOpInfo = aliasToOpInfo.values().iterator().next();

    Operator bodyOpInfo = genBodyPlan(qb, srcOpInfo);
    LOG.debug("Created Plan for Query Block " + qb.getId());
    
    this.qb = qb;
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

    if (qb.isSelectStarQuery()) {
      Iterator<Map.Entry<String, Table>> iter = qb.getMetaData().getAliasToTable().entrySet().iterator();
      Table tab = ((Map.Entry<String, Table>)iter.next()).getValue();
      if (!tab.isPartitioned()) {
        if (qb.getParseInfo().getDestToWhereExpr().isEmpty())
          fetch = new fetchWork(tab.getPath(), Utilities.getTableDesc(tab), qb.getParseInfo().getOuterQueryLimit()); 
      }
      else {
        if (aliasToPruner.size() == 1) {
          Iterator<Map.Entry<String, PartitionPruner>> iterP = aliasToPruner.entrySet().iterator();
          PartitionPruner pr = ((Map.Entry<String, PartitionPruner>)iterP.next()).getValue();
          if (pr.containsPartitionCols()) {
            List<Path> listP = new ArrayList<Path>();
            List<partitionDesc> partP = new ArrayList<partitionDesc>();
            Set<Partition> parts = null;
            try {
              parts = pr.prune();
              Iterator<Partition> iterParts = parts.iterator();
              while (iterParts.hasNext()) {
              	Partition part = iterParts.next();
                listP.add(part.getPartitionPath());
                partP.add(Utilities.getPartitionDesc(part));
              }
              fetch = new fetchWork(listP, partP, qb.getParseInfo().getOuterQueryLimit());
            } catch (HiveException e) {
              // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
              LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
              throw new SemanticException(e.getMessage(), e);
            }
          }
        }
      }
      if (fetch != null) {
        fetchTask = TaskFactory.get(fetch, this.conf);
        setFetchTask(fetchTask);
        return;
      }
    }

    // In case of a select, use a fetch task instead of a move task
    if (qb.getIsQuery()) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1))
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      String cols = loadFileWork.get(0).getColumns();
    
      fetch = new fetchWork(new Path(loadFileWork.get(0).getSourceDir()),
      			                new tableDesc(MetadataTypedColumnsetSerDe.class, TextInputFormat.class,
                 			                		IgnoreKeyTextOutputFormat.class,
      			                           		Utilities.makeProperties(
      			                                org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
      			                                "columns", cols)),    
      			                qb.getParseInfo().getOuterQueryLimit());    

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
      setKeyAndValueDesc(plan, topOp);
      LOG.debug("Created Map Work for " + alias_id);
    }
  }

  private void setKeyAndValueDesc(mapredWork plan, Operator<? extends Serializable> topOp) {
    if (topOp instanceof ReduceSinkOperator) {
      ReduceSinkOperator rs = (ReduceSinkOperator)topOp;
      plan.setKeyDesc(rs.getConf().getKeySerializeInfo());
      int tag = Math.max(0, rs.getConf().getTag());
      List<tableDesc> tagToSchema = plan.getTagToValueDesc();
      while (tag + 1 > tagToSchema.size()) {
        tagToSchema.add(null);
      }
      tagToSchema.set(tag, rs.getConf().getValueSerializeInfo());
    } else {
      List<Operator<? extends Serializable>> children = topOp.getChildOperators(); 
      if (children != null) {
        for(Operator<? extends Serializable> op: children) {
          setKeyAndValueDesc(plan, op);
        }
      }
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

            tableDesc tt_desc = PlanUtils.getBinaryTableDesc(
                PlanUtils.getFieldSchemasFromRowSchema(op.getSchema(), "temporarycol")); 

            // Create a file sink operator for this file name
            Operator<? extends Serializable> fs_op = putOpInsertMap(OperatorFactory.get(new fileSinkDesc(taskTmpDir, tt_desc),
                                                                                        op.getSchema()), null);

            // replace the reduce child with this operator
            childOps.set(i, fs_op);
            
            List<Operator<? extends Serializable>> parent = new ArrayList<Operator<? extends Serializable>>();
            parent.add(op);
            fs_op.setParentOperators(parent);

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
            setKeyAndValueDesc(cplan, child);

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
  public Phase1Ctx initPhase1Ctx() {

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
    work.setTagToValueDesc(new ArrayList<tableDesc>());
    work.setReducer(null);

    return work;
  }

  private boolean pushSelect(Operator<? extends Serializable> op, List<String> colNames) {
    if (opParseCtx.get(op).getRR().getColumnInfos().size() == colNames.size()) return false;
    return true;
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(CommonTree ast, Context ctx) throws SemanticException {
    this.ctx = ctx;
    reset();

    QB qb = new QB(null, null, false);
    this.qb = qb;
    this.ast = ast;

    LOG.info("Starting Semantic Analysis");
    doPhase1(ast, qb, initPhase1Ctx());
    LOG.info("Completed phase 1 of Semantic Analysis");

    getMetaData(qb);
    LOG.info("Completed getting MetaData in Semantic Analysis");

    genPlan(qb);

    ParseContext pCtx = new ParseContext(conf, qb, ast, aliasToPruner, aliasToSamplePruner, topOps, 
    		                                 topSelOps, opParseCtx, loadTableWork, loadFileWork, ctx);
  
    Optimizer optm = new Optimizer();
    optm.setPctx(pCtx);
    optm.initialize();
    pCtx = optm.optimize();
    init(pCtx);
    qb = pCtx.getQB();

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


  /**
   * Generates and expression node descriptor for the expression passed in the arguments. This
   * function uses the row resolver and the metadata informatinon that are passed as arguments
   * to resolve the column names to internal names.
   * @param qbm The metadata infromation for the query block
   * @param expr The expression
   * @param input The row resolver
   * @return exprNodeDesc
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private exprNodeDesc genExprNodeDesc(QBMetaData qbm, CommonTree expr, RowResolver input)
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

        String tabAlias = null;
        String colName = null;
        if (expr.getChildCount() != 1) {
          tabAlias = unescapeIdentifier(expr.getChild(0).getText());
          colName = unescapeIdentifier(expr.getChild(1).getText());
        }
        else {
          colName = unescapeIdentifier(expr.getChild(0).getText());
        }

        if (colName == null) {
          throw new SemanticException(ErrorMsg.INVALID_XPATH.getMsg(expr));
        }

        colInfo = input.get(tabAlias, colName);

        if (colInfo == null && input.getIsExprResolver()) {
          throw new SemanticException(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(expr));
        }         
        else if (tabAlias != null && !input.hasTableAlias(tabAlias)) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(expr.getChild(0)));
        } else if (colInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(tabAlias == null? expr.getChild(0) : expr.getChild(1)));
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
          children.add(genExprNodeDesc(qbm, (CommonTree)expr.getChild(ci), input));
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
            TypeInfoFactory.getPrimitiveTypeInfo(String.class), unescapeIdentifier(expr.getText()));
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
  
  /**
   * Gets the table Alias for the column from the column name. This function throws
   * and exception in case the same column name is present in multiple table. The exception 
   * message indicates that the ambiguity could not be resolved.
   * 
   * @param qbm The metadata where the function looks for the table alias
   * @param colName The name of the non aliased column
   * @param pt The parse tree corresponding to the column(this is used for error reporting)
   * @return String
   * @throws SemanticException
   */
  static String getTabAliasForCol(QBMetaData qbm, String colName, CommonTree pt) 
  throws SemanticException {
	  String tabAlias = null;
	  boolean found = false;
	  
	  for(Map.Entry<String, Table> ent: qbm.getAliasToTable().entrySet()) {
		  for(FieldSchema field: ent.getValue().getAllCols()) {
			  if (colName.equalsIgnoreCase(field.getName())) {
				if (found) {
					throw new SemanticException(ErrorMsg.AMBIGOUS_COLUMN.getMsg(pt));
				}
				
				found = true;
				tabAlias = ent.getKey();
			  }
		  }
	  }
	  return tabAlias;
  }
}
