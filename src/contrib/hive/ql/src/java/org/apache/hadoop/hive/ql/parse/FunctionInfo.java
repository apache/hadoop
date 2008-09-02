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

import java.lang.Class;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The type information returned by the TypeRegistry
 *
 **/

public class FunctionInfo {
  private TypeInfo returnType;
  private String UDFName;
  private boolean hasUDF;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(FunctionInfo.class.getName());
  
  public FunctionInfo(TypeInfo returnType, String UDFName) {
    this.returnType = returnType;
    this.UDFName = UDFName;
    this.hasUDF = (this.UDFName != null);
  }

  public FunctionInfo(Class<?> returnClass, String UDFName) {
    this(new TypeInfo(returnClass), UDFName);
  }

  public TypeInfo getReturnType() {
    return returnType;
  }

  public String getUDFName() {
    return UDFName;
  }

  public boolean hasUDF() {
    return hasUDF;
  }
}
