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

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDAF;

public class aggregationDesc implements java.io.Serializable {
  private static final long serialVersionUID = 1L;
  private Class<? extends UDAF> aggregationClass;
  private java.util.ArrayList<exprNodeDesc> parameters;
  private boolean distinct;
  public aggregationDesc() {}
  public aggregationDesc(
    final Class<? extends UDAF> aggregationClass,
    final java.util.ArrayList<exprNodeDesc> parameters,
    final boolean distinct) {
    this.aggregationClass = aggregationClass;
    this.parameters = parameters;
    this.distinct = distinct;
  }
  public Class<? extends UDAF> getAggregationClass() {
    return this.aggregationClass;
  }
  public void setAggregationClass(final Class<? extends UDAF> aggregationClass) {
    this.aggregationClass = aggregationClass;
  }
  public java.util.ArrayList<exprNodeDesc> getParameters() {
    return this.parameters;
  }
  public void setParameters(final java.util.ArrayList<exprNodeDesc> parameters) {
    this.parameters=parameters;
  }
  public boolean getDistinct() {
    return this.distinct;
  }
  public void setDistinct(final boolean distinct) {
    this.distinct = distinct;
  }
  
  @explain(displayName="expr")
  public String getExprString() {
    FunctionInfo fI = FunctionRegistry.getInfo(aggregationClass);
    StringBuilder sb = new StringBuilder();
    sb.append(fI.getDisplayName());
    sb.append("(");
    if (distinct) {
      sb.append("DISTINCT ");
    }
    boolean first = true;
    for(exprNodeDesc exp: parameters) {
      if (!first) {
        sb.append(", ");
      }
      
      sb.append(exp.getExprString());
      first = false;
    }
    sb.append(")");
    
    return sb.toString();
  }
}
