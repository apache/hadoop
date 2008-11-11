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

package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import java.io.Serializable;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * Implementation of one of the rule-based optimization steps. ColumnPruner gets the current operator tree. The tree is traversed to find out the columns used 
 * for all the base tables. If all the columns for a table are not used, a select is pushed on top of that table (to select only those columns). Since this 
 * changes the row resolver, the tree is built again. This can be optimized later to patch the tree. 
 */
public class ColumnPruner implements Transform {
  private ParseContext pctx;
  
  /**
   * empty constructor
   */
	public ColumnPruner() {
    pctx = null;
	}

	/**
	 * Whether some column pruning needs to be done
	 * @param op Operator for the base table
	 * @param colNames columns needed by the query
	 * @return boolean
	 */
  private boolean pushSelect(Operator<? extends Serializable> op, List<String> colNames) {
    if (pctx.getOpParseCtx().get(op).getRR().getColumnInfos().size() == colNames.size()) return false;
    return true;
  }

  /**
   * update the map between operator and row resolver
   * @param op operator being inserted
   * @param rr row resolver of the operator
   * @return
   */
  @SuppressWarnings("nls")
  private Operator<? extends Serializable> putOpInsertMap(Operator<? extends Serializable> op, RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    pctx.getOpParseCtx().put(op, ctx);
    return op;
  }

  /**
   * insert a select to include only columns needed by the query
   * @param input operator for the base table
   * @param colNames columns needed
   * @return
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genSelectPlan(Operator input, List<String> colNames) 
    throws SemanticException {

    RowResolver inputRR  = pctx.getOpParseCtx().get(input).getRR();
    RowResolver outputRR = new RowResolver();
    ArrayList<exprNodeDesc> col_list = new ArrayList<exprNodeDesc>();
    
    // Iterate over the selects
    for (int pos = 0; pos < colNames.size(); pos++) {
      String   internalName = colNames.get(pos);
      String[] colName      = inputRR.reverseLookup(internalName);
      ColumnInfo in = inputRR.get(colName[0], colName[1]);
      outputRR.put(colName[0], colName[1], 
                   new ColumnInfo((Integer.valueOf(pos)).toString(), in.getType()));
      col_list.add(new exprNodeColumnDesc(in.getType(), internalName));
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
      new selectDesc(col_list), new RowSchema(outputRR.getColumnInfos()), input), outputRR);

    return output;
  }

  /**
   * reset parse context
   * @param pctx parse context
   */
  private void resetParseContext(ParseContext pctx) {
    pctx.getAliasToPruner().clear();
    pctx.getAliasToSamplePruner().clear();
    pctx.getLoadTableWork().clear();
    pctx.getLoadFileWork().clear();
    Iterator<Operator<? extends Serializable>> iter = pctx.getOpParseCtx().keySet().iterator();
    while (iter.hasNext()) {
      Operator<? extends Serializable> op = iter.next();
      if ((!pctx.getTopOps().containsValue(op)) && (!pctx.getTopSelOps().containsValue(op)))
        iter.remove();
    }
  }
	
  /**
   * Transform the query tree. For each table under consideration, check if all columns are needed. If not, only select the operators needed at
   * the beginning and proceed 
   */
	public ParseContext transform(ParseContext pactx) throws SemanticException {
    this.pctx = pactx;
    boolean done = true;
    // generate useful columns for all the sources so that they can be pushed immediately after the table scan
    for (String alias_id : pctx.getTopOps().keySet()) {
      Operator<? extends Serializable> topOp = pctx.getTopOps().get(alias_id);
      
      // Scan the tree bottom-up and generate columns needed for the top operator
      List<String> colNames = topOp.genColLists(pctx.getOpParseCtx());

      // do we need to push a SELECT - all the columns of the table are not used
      if (pushSelect(topOp, colNames)) {
        topOp.setChildOperators(null);

        // Generate a select and make it a child of the table scan
        Operator select = genSelectPlan(topOp, colNames);
        pctx.getTopSelOps().put(alias_id, select);
        done = false;
      }
    }

    // a select was pushed on top of the table. The old plan is no longer valid. Generate the plan again.
    // The current tables and the select pushed above (after column pruning) are maintained in the parse context.
    if (!done) {
      SemanticAnalyzer sem = (SemanticAnalyzer)SemanticAnalyzerFactory.get(pctx.getConf(), pctx.getParseTree());
      
      resetParseContext(pctx);
      sem.init(pctx);
    	QB qb = new QB(null, null, false);
    	
    	sem.doPhase1(pctx.getParseTree(), qb, sem.initPhase1Ctx());
    	sem.getMetaData(qb);
    	sem.genPlan(qb);
      pctx = sem.getParseContext();
   	}	
    return pctx;
  }
}
