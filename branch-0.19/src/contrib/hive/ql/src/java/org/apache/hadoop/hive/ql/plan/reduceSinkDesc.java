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
  // these are the expressions that go into the reduce key
  private java.util.ArrayList<exprNodeDesc> keyCols;
  private java.util.ArrayList<exprNodeDesc> valueCols;
  // Describe how to serialize the key
  private tableDesc keySerializeInfo;
  // Describe how to serialize the value
  private tableDesc valueSerializeInfo;
  
  private int tag;
  
  // The partition key will be the first #numPartitionFields of keyCols
  // If the value is 0, then all data will go to a single reducer
  // If the value is -1, then data will go to a random reducer 
  private int numPartitionFields;
  
  private boolean inferNumReducers;
  private int numReducers;

  public reduceSinkDesc() { }

  public reduceSinkDesc
    (java.util.ArrayList<exprNodeDesc> keyCols,
     java.util.ArrayList<exprNodeDesc> valueCols,
     int tag,
     int numPartitionFields,
     int numReducers,
     boolean inferNumReducers,
     final tableDesc keySerializeInfo,
     final tableDesc valueSerializeInfo) {
    this.keyCols = keyCols;
    this.valueCols = valueCols;
    this.tag = tag;
    this.numReducers = numReducers;
    this.inferNumReducers = inferNumReducers;
    this.numPartitionFields = numPartitionFields;
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
  
  @explain(displayName="# partition fields")
  public int getNumPartitionFields() {
    return this.numPartitionFields;
  }
  public void setNumPartitionFields(int numPartitionFields) {
    this.numPartitionFields = numPartitionFields;
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
