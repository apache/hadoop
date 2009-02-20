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

import org.antlr.runtime.tree.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of the parse information related to a query block
 *
 **/

public class QBParseInfo {

  private boolean isSubQ;
  private boolean canOptTopQ;
  private String alias;
  private CommonTree joinExpr;
  private HashMap<String, CommonTree> aliasToSrc;
  private HashMap<String, CommonTree> nameToDest;
  private HashMap<String, TableSample> nameToSample;
  private HashMap<String, CommonTree> destToSelExpr;
  private HashMap<String, CommonTree> destToWhereExpr;
  private HashMap<String, CommonTree> destToGroupby;
  private HashMap<String, CommonTree> destToClusterby;
  private HashMap<String, Integer>    destToLimit;
  private int outerQueryLimit;

  // used by GroupBy
  private HashMap<String, HashMap<String, CommonTree> > destToAggregationExprs;
  private HashMap<String, CommonTree> destToDistinctFuncExpr;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBParseInfo.class.getName());
  
  public QBParseInfo(String alias, boolean isSubQ) {
    this.aliasToSrc = new HashMap<String, CommonTree>();
    this.nameToDest = new HashMap<String, CommonTree>();
    this.nameToSample = new HashMap<String, TableSample>();
    this.destToSelExpr = new HashMap<String, CommonTree>();
    this.destToWhereExpr = new HashMap<String, CommonTree>();
    this.destToGroupby = new HashMap<String, CommonTree>();
    this.destToClusterby = new HashMap<String, CommonTree>();
    this.destToLimit = new HashMap<String, Integer>();
    
    this.destToAggregationExprs = new HashMap<String, HashMap<String, CommonTree> >();
    this.destToDistinctFuncExpr = new HashMap<String, CommonTree>();
    
    this.alias = alias;
    this.isSubQ = isSubQ;
    this.canOptTopQ = false;
    this.outerQueryLimit = -1;
  }

  public void setAggregationExprsForClause(String clause, HashMap<String, CommonTree> aggregationTrees) {
    this.destToAggregationExprs.put(clause, aggregationTrees);
  }

  public HashMap<String, CommonTree> getAggregationExprsForClause(String clause) {
    return this.destToAggregationExprs.get(clause);
  }

  public void setDistinctFuncExprForClause(String clause, CommonTree ast) {
    this.destToDistinctFuncExpr.put(clause, ast);
  }
  
  public CommonTree getDistinctFuncExprForClause(String clause) {
    return this.destToDistinctFuncExpr.get(clause);
  }
  
  public void setSelExprForClause(String clause, CommonTree ast) {
    this.destToSelExpr.put(clause, ast);
  }

  public void setWhrExprForClause(String clause, CommonTree ast) {
    this.destToWhereExpr.put(clause, ast);
  }

  public void setGroupByExprForClause(String clause, CommonTree ast) {
    this.destToGroupby.put(clause, ast);
  }

  public void setDestForClause(String clause, CommonTree ast) {
    this.nameToDest.put(clause, ast);
  }

  public void setClusterByExprForClause(String clause, CommonTree ast) {
    this.destToClusterby.put(clause, ast);
  }

  public void setSrcForAlias(String alias, CommonTree ast) {
    this.aliasToSrc.put(alias.toLowerCase(), ast);
  }

  public Set<String> getClauseNames() {
    return this.destToSelExpr.keySet();
  }

  public Set<String> getClauseNamesForDest() {
    return this.nameToDest.keySet();
  }

  public CommonTree getDestForClause(String clause) {
    return this.nameToDest.get(clause);
  }

  public CommonTree getWhrForClause(String clause) {
    return this.destToWhereExpr.get(clause);
  }

  public CommonTree getGroupByForClause(String clause) {
    return this.destToGroupby.get(clause);
  }

  public CommonTree getSelForClause(String clause) {
    return this.destToSelExpr.get(clause);
  }

  public CommonTree getClusterByForClause(String clause) {
    return this.destToClusterby.get(clause);
  }

  public CommonTree getSrcForAlias(String alias) {
    return this.aliasToSrc.get(alias.toLowerCase());
  }

  public String getAlias() {
    return this.alias;
  }

  public boolean getIsSubQ() {
    return this.isSubQ;
  }

  public boolean getCanOptTopQ() {
    return this.canOptTopQ;
  }

  public void setCanOptTopQ(boolean canOptTopQ) {
    this.canOptTopQ = canOptTopQ;
  }
  
  public CommonTree getJoinExpr() {
    return this.joinExpr;
  }

  public void setJoinExpr(CommonTree joinExpr) {
    this.joinExpr = joinExpr;
  }

  public TableSample getTabSample(String alias) {
    return this.nameToSample.get(alias.toLowerCase());
  }
  
  public void setTabSample(String alias, TableSample tableSample) {
    this.nameToSample.put(alias.toLowerCase(), tableSample);
  }

  public void setDestLimit(String dest, Integer limit) {
    this.destToLimit.put(dest, limit);
  }

  public Integer getDestLimit(String dest) {
    return this.destToLimit.get(dest);
  }

	/**
	 * @return the outerQueryLimit
	 */
	public int getOuterQueryLimit() {
		return outerQueryLimit;
	}

	/**
	 * @param outerQueryLimit the outerQueryLimit to set
	 */
	public void setOuterQueryLimit(int outerQueryLimit) {
		this.outerQueryLimit = outerQueryLimit;
	}

  public boolean isSelectStarQuery() {
    if (isSubQ || 
       (joinExpr != null) ||
       (!nameToSample.isEmpty()) ||
       (!destToWhereExpr.isEmpty()) ||
       (!destToGroupby.isEmpty()) ||
       (!destToClusterby.isEmpty()))
      return false;
    
    Iterator<Map.Entry<String, HashMap<String, CommonTree>>> aggrIter = destToAggregationExprs.entrySet().iterator();
    while (aggrIter.hasNext()) {
      HashMap<String, CommonTree> h = aggrIter.next().getValue();
      if ((h != null) && (!h.isEmpty()))
        return false;
    }
      	
    if (!destToDistinctFuncExpr.isEmpty()) {
      Iterator<Map.Entry<String, CommonTree>> distn = destToDistinctFuncExpr.entrySet().iterator();
      while (distn.hasNext()) {
        CommonTree ct = distn.next().getValue();
        if (ct != null) 
          return false;
      }
    }
        
    Iterator<Map.Entry<String, CommonTree>> iter = nameToDest.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, CommonTree> entry = iter.next();
      CommonTree v = entry.getValue();
      if (!(((CommonTree)v.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE))
        return false;
    }
      	
    iter = destToSelExpr.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, CommonTree> entry = iter.next();
      CommonTree selExprList = entry.getValue();
      // Iterate over the selects
      for (int i = 0; i < selExprList.getChildCount(); ++i) {
        
        // list of the columns
        CommonTree selExpr = (CommonTree) selExprList.getChild(i);
        CommonTree sel = (CommonTree)selExpr.getChild(0);
        
        if (sel.getToken().getType() != HiveParser.TOK_ALLCOLREF)
          return false;
      }
    }

    return true;
  }
  
}
