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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Select operator implementation
 **/
public class SelectOperator extends Operator <selectDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] eval;

  transient ArrayList<Object> output;
  transient ArrayList<ObjectInspector> outputFieldObjectInspectors;
  transient ObjectInspector outputObjectInspector;
  transient InspectableObject tempInspectableObject;
  
  boolean firstRow;
  
  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      ArrayList<exprNodeDesc> colList = conf.getColList();
      eval = new ExprNodeEvaluator[colList.size()];
      for(int i=0; i<colList.size(); i++) {
        assert(colList.get(i) != null);
        eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
      }
      output = new ArrayList<Object>(eval.length);
      outputFieldObjectInspectors = new ArrayList<ObjectInspector>(eval.length);
      for(int j=0; j<eval.length; j++) {
        output.add(null);
        outputFieldObjectInspectors.add(null);
      }
      tempInspectableObject = new InspectableObject();      
      firstRow = true;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void process(Object row, ObjectInspector rowInspector)
      throws HiveException {
    for(int i=0; i<eval.length; i++) {
      eval[i].evaluate(row, rowInspector, tempInspectableObject);
      output.set(i, tempInspectableObject.o);
      if (firstRow) {
        outputFieldObjectInspectors.set(i, tempInspectableObject.oi);
      }
    }
    if (firstRow) {
      firstRow = false;
      ArrayList<String> fieldNames = new ArrayList<String>(eval.length);
      for(int i=0; i<eval.length; i++) {
        fieldNames.add(Integer.valueOf(i).toString());
      }
      outputObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        fieldNames, outputFieldObjectInspectors);
    }
    forward(output, outputObjectInspector);
  }

  private List<String> getColsFromExpr(HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    List<String> cols = new ArrayList<String>();
    ArrayList<exprNodeDesc> exprList = conf.getColList();
    for (exprNodeDesc expr : exprList)
      cols = Utilities.mergeUniqElems(cols, expr.getCols());
    List<Integer> listExprs = new ArrayList<Integer>();
    for (int pos = 0; pos < exprList.size(); pos++)
      listExprs.add(new Integer(pos));
    OpParseContext ctx = opParseCtx.get(this);
    ctx.setColNames(cols);
    opParseCtx.put(this, ctx);
    return cols;
  }

  private List<String> getColsFromExpr(List<String> colList, 
                                       HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
  	if (colList.isEmpty())
  		return getColsFromExpr(opParseCtx);
  	
    List<String> cols = new ArrayList<String>();
    ArrayList<exprNodeDesc> selectExprs = conf.getColList();
    List<Integer> listExprs = new ArrayList<Integer>();

    for (String col : colList) {
      // col is the internal name i.e. position within the expression list
      Integer pos = new Integer(col);
      exprNodeDesc expr = selectExprs.get(pos.intValue());
      cols = Utilities.mergeUniqElems(cols, expr.getCols());
      listExprs.add(pos);
    }

    OpParseContext ctx = opParseCtx.get(this);
    ctx.setColNames(cols);
    opParseCtx.put(this, ctx);
    return cols;
  }

  public List<String> genColLists(HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) 
    throws SemanticException {
    List<String> cols = new ArrayList<String>();
    
    for(Operator<? extends Serializable> o: childOperators) {
      // if one of my children is a fileSink, return everything
      if ((o instanceof FileSinkOperator) || (o instanceof ScriptOperator))
        return getColsFromExpr(opParseCtx);

      cols = Utilities.mergeUniqElems(cols, o.genColLists(opParseCtx));
    }

    if (conf.isSelectStar())
      // The input to the select does not matter. Go over the expressions and return the ones which have a marked column
      return getColsFromExpr(cols, opParseCtx);
    
    return getColsFromExpr(opParseCtx);
  }

}
