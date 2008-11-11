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

@explain(displayName="Reduce Output Operator")
public class reduceSinkDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * Key columns are passed to reducer in the "key". 
   */
  private java.util.ArrayList<exprNodeDesc> keyCols;
  /**
   * Value columns are passed to reducer in the "value". 
   */
  private java.util.ArrayList<exprNodeDesc> valueCols;
  /** 
   * Describe how to serialize the key.
   */
  private tableDesc keySerializeInfo;
  /**
   * Describe how to serialize the value.
   */
  private tableDesc valueSerializeInfo;
  
  /**
   * The tag for this reducesink descriptor.
   */
  private int tag;
  
  /**
   * The partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  private java.util.ArrayList<exprNodeDesc> partitionCols;
  
  private boolean inferNumReducers;
  private int numReducers;

  public reduceSinkDesc() { }

  public reduceSinkDesc
    (java.util.ArrayList<exprNodeDesc> keyCols,
     java.util.ArrayList<exprNodeDesc> valueCols,
     int tag,
     java.util.ArrayList<exprNodeDesc> partitionCols,
     int numReducers,
     boolean inferNumReducers,
     final tableDesc keySerializeInfo,
     final tableDesc valueSerializeInfo) {
    this.keyCols = keyCols;
    this.valueCols = valueCols;
    this.tag = tag;
    this.numReducers = numReducers;
    this.inferNumReducers = inferNumReducers;
    this.partitionCols = partitionCols;
    this.keySerializeInfo = keySerializeInfo;
    this.valueSerializeInfo = valueSerializeInfo;
  }

  @explain(displayName="key expressions")
  public java.util.ArrayList<exprNodeDesc> getKeyCols() {
    return this.keyCols;
  }
  public void setKeyCols
    (final java.util.ArrayList<exprNodeDesc> keyCols) {
    this.keyCols=keyCols;
  }

  @explain(displayName="value expressions")
  public java.util.ArrayList<exprNodeDesc> getValueCols() {
    return this.valueCols;
  }
  public void setValueCols
    (final java.util.ArrayList<exprNodeDesc> valueCols) {
    this.valueCols=valueCols;
  }
  
  @explain(displayName="Map-reduce partition columns")
  public java.util.ArrayList<exprNodeDesc> getPartitionCols() {
    return this.partitionCols;
  }
  public void setPartitionCols(final java.util.ArrayList<exprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }
  
  @explain(displayName="tag")
  public int getTag() {
    return this.tag;
  }
  public void setTag(int tag) {
    this.tag = tag;
  }

  public boolean getInferNumReducers() {
    return this.inferNumReducers;
  }
  public void setInferNumReducers(boolean inferNumReducers) {
    this.inferNumReducers = inferNumReducers;
  }

  public int getNumReducers() {
    return this.numReducers;
  }
  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }

  public tableDesc getKeySerializeInfo() {
    return keySerializeInfo;
  }

  public void setKeySerializeInfo(tableDesc keySerializeInfo) {
    this.keySerializeInfo = keySerializeInfo;
  }

  public tableDesc getValueSerializeInfo() {
    return valueSerializeInfo;
  }

  public void setValueSerializeInfo(tableDesc valueSerializeInfo) {
    this.valueSerializeInfo = valueSerializeInfo;
  }
  
}
