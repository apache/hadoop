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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Join operator Descriptor implementation.
 * 
 */
@explain(displayName="Join Operator")
public class joinDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final int INNER_JOIN = 0;
  public static final int LEFT_OUTER_JOIN = 1;
  public static final int RIGHT_OUTER_JOIN = 2;
  public static final int FULL_OUTER_JOIN = 3;

  // alias to columns mapping
  private Map<Byte, ArrayList<exprNodeDesc>> exprs;
  
  // No outer join involved
  private boolean noOuterJoin;

  private joinCond[] conds;

  public joinDesc() { }
  
  public joinDesc(final Map<Byte, ArrayList<exprNodeDesc>> exprs, final boolean noOuterJoin, final joinCond[] conds) {
    this.exprs = exprs;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;
  }
  
  public joinDesc(final Map<Byte, ArrayList<exprNodeDesc>> exprs) {
    this.exprs = exprs;
    this.noOuterJoin = true;
    this.conds = null;
  }

  public joinDesc(final Map<Byte, ArrayList<exprNodeDesc>> exprs, final joinCond[] conds) {
    this.exprs = exprs;
    this.noOuterJoin = false;
    this.conds = conds;
  }
  
  public Map<Byte, ArrayList<exprNodeDesc>> getExprs() {
    return this.exprs;
  }

  @explain(displayName="condition expressions")
  public Map<Byte, String> getExprsStringMap() {
    if (getExprs() == null) {
      return null;
    }
    
    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();
    
    for(Map.Entry<Byte, ArrayList<exprNodeDesc>> ent: getExprs().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
        for(exprNodeDesc expr: ent.getValue()) {
          if (!first) {
            sb.append(" ");
          }
          
          first = false;
          sb.append("{");
          sb.append(expr.getExprString());
          sb.append("}");
        }
      }
      ret.put(ent.getKey(), sb.toString());
    }
    
    return ret;
  }
  
  public void setExprs(final Map<Byte, ArrayList<exprNodeDesc>> exprs) {
    this.exprs = exprs;
  }

  public boolean getNoOuterJoin() {
    return this.noOuterJoin;
  }

  public void setNoOuterJoin(final boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  @explain(displayName="condition map")
  public List<joinCond> getCondsList() {
    if (conds == null) {
      return null;
    }

    ArrayList<joinCond> l = new ArrayList<joinCond>();
    for(joinCond cond: conds) {
      l.add(cond);
    }

    return l;
  }

  public joinCond[] getConds() {
    return this.conds;
  }

  public void setConds(final joinCond[] conds) {
    this.conds = conds;
  }

}
