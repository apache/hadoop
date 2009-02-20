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

public class FunctionInfo {

  private String displayName;

  private OperatorType opType;

  private boolean isOperator;
  
  private Class<? extends UDF> udfClass;

  private Class<? extends UDAF> udafClass;

  public static enum OperatorType { NO_OP, PREFIX, INFIX, POSTFIX };

  public FunctionInfo(String displayName, Class<? extends UDF> udfClass, Class<? extends UDAF> udafClass) {
    assert(udfClass == null || udafClass == null);
    this.displayName = displayName;
    opType = OperatorType.NO_OP;
    isOperator = false;
    this.udfClass = udfClass;
    this.udafClass = udafClass;
  }

  public FunctionInfo(String displayName, OperatorType opType, Class<? extends UDF> udfClass) {
    this.displayName = displayName;
    this.opType = opType;
    this.udfClass = udfClass;
    this.udafClass = null;
  }

  public boolean isAggFunction() {
    return (udafClass != null && udfClass == null);
  }

  public boolean isOperator() {
    return isOperator;
  }

  public void setIsOperator(boolean val) {
    isOperator = val;
  }
  
  public void setOpType(OperatorType opt) {
    opType = opt;
  }
  
  public OperatorType getOpType() {
    return opType;
  }

  public Class<? extends UDF> getUDFClass() {
    return udfClass;
  }

  public Class<? extends UDAF> getUDAFClass() {
    return udafClass;
  }
  
  public String getDisplayName() {
    return displayName;
  }
}
