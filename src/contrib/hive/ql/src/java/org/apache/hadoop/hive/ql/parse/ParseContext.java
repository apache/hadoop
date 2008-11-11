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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Parse Context: The current parse context. This is passed to the optimizer
 * which then transforms the operator tree using the parse context. All the
 * optimizations are performed sequentially and then the new parse context
 * populated. Note that since the parse context contains the operator tree, it
 * can be easily retrieved by the next optimization step or finally for task
 * generation after the plan has been completely optimized.
 * 
 **/

public class ParseContext {
  private QB qb;
  private CommonTree ast;
  private HashMap<String, PartitionPruner> aliasToPruner;
  private HashMap<String, SamplePruner> aliasToSamplePruner;
  private HashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;
  private Context ctx;
  private HiveConf conf;

  /**
   * @param qb
   *          current QB
   * @param ast
   *          current parse tree
   * @param aliasToPruner
   *          partition pruner list
   * @param aliasToSamplePruner
   *          sample pruner list
   * @param loadFileWork
   *          list of destination files being loaded
   * @param loadTableWork
   *          list of destination tables being loaded
   * @param opParseCtx
   *          operator parse context - contains a mapping from operator to
   *          operator parse state (row resolver etc.)
   * @param topOps
   *          list of operators for the top query
   * @param topSelOps
   *          list of operators for the selects introduced for column pruning
   */
  public ParseContext(HiveConf conf, QB qb, CommonTree ast,
      HashMap<String, PartitionPruner> aliasToPruner,
      HashMap<String, SamplePruner> aliasToSamplePruner,
      HashMap<String, Operator<? extends Serializable>> topOps,
      HashMap<String, Operator<? extends Serializable>> topSelOps,
      HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx,
      List<loadTableDesc> loadTableWork, List<loadFileDesc> loadFileWork,
      Context ctx) {
    this.conf = conf;
    this.qb = qb;
    this.ast = ast;
    this.aliasToPruner = aliasToPruner;
    this.aliasToSamplePruner = aliasToSamplePruner;
    this.loadFileWork = loadFileWork;
    this.loadTableWork = loadTableWork;
    this.opParseCtx = opParseCtx;
    this.topOps = topOps;
    this.topSelOps = topSelOps;
    this.ctx = ctx;
  }

  /**
   * @return the qb
   */
  public QB getQB() {
    return qb;
  }

  /**
   * @param qb
   *          the qb to set
   */
  public void setQB(QB qb) {
    this.qb = qb;
  }

  /**
   * @return the context
   */
  public Context getContext() {
    return ctx;
  }

  /**
   * @param ctx
   *          the context to set
   */
  public void setContext(Context ctx) {
    this.ctx = ctx;
  }

  /**
   * @return the hive conf
   */
  public HiveConf getConf() {
    return conf;
  }

  /**
   * @param conf
   *          the conf to set
   */
  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  /**
   * @return the ast
   */
  public CommonTree getParseTree() {
    return ast;
  }

  /**
   * @param ast
   *          the parsetree to set
   */
  public void setParseTree(CommonTree ast) {
    this.ast = ast;
  }

  /**
   * @return the aliasToPruner
   */
  public HashMap<String, PartitionPruner> getAliasToPruner() {
    return aliasToPruner;
  }

  /**
   * @param aliasToPruner
   *          the aliasToPruner to set
   */
  public void setAliasToPruner(HashMap<String, PartitionPruner> aliasToPruner) {
    this.aliasToPruner = aliasToPruner;
  }

  /**
   * @return the aliasToSamplePruner
   */
  public HashMap<String, SamplePruner> getAliasToSamplePruner() {
    return aliasToSamplePruner;
  }

  /**
   * @param aliasToSamplePruner
   *          the aliasToSamplePruner to set
   */
  public void setAliasToSamplePruner(
      HashMap<String, SamplePruner> aliasToSamplePruner) {
    this.aliasToSamplePruner = aliasToSamplePruner;
  }

  /**
   * @return the topOps
   */
  public HashMap<String, Operator<? extends Serializable>> getTopOps() {
    return topOps;
  }

  /**
   * @param topOps
   *          the topOps to set
   */
  public void setTopOps(HashMap<String, Operator<? extends Serializable>> topOps) {
    this.topOps = topOps;
  }

  /**
   * @return the topSelOps
   */
  public HashMap<String, Operator<? extends Serializable>> getTopSelOps() {
    return topSelOps;
  }

  /**
   * @param topSelOps
   *          the topSelOps to set
   */
  public void setTopSelOps(
      HashMap<String, Operator<? extends Serializable>> topSelOps) {
    this.topSelOps = topSelOps;
  }

  /**
   * @return the opParseCtx
   */
  public HashMap<Operator<? extends Serializable>, OpParseContext> getOpParseCtx() {
    return opParseCtx;
  }

  /**
   * @param opParseCtx
   *          the opParseCtx to set
   */
  public void setOpParseCtx(
      HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    this.opParseCtx = opParseCtx;
  }

  /**
   * @return the loadTableWork
   */
  public List<loadTableDesc> getLoadTableWork() {
    return loadTableWork;
  }

  /**
   * @param loadTableWork
   *          the loadTableWork to set
   */
  public void setLoadTableWork(List<loadTableDesc> loadTableWork) {
    this.loadTableWork = loadTableWork;
  }

  /**
   * @return the loadFileWork
   */
  public List<loadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  /**
   * @param loadFileWork
   *          the loadFileWork to set
   */
  public void setLoadFileWork(List<loadFileDesc> loadFileWork) {
    this.loadFileWork = loadFileWork;
  }
}
