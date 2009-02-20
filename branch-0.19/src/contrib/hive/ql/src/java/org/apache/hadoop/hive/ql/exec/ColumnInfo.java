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

import java.lang.Class;
import java.io.*;

import org.apache.hadoop.hive.ql.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;

/**
 * Implementation for ColumnInfo which contains the internal name for the 
 * column (the one that is used by the operator to access the column) and
 * the type (identified by a java class).
 **/

public class ColumnInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String internalName;

  /**
   * isVirtual indicates whether the column is a virtual column or not. Virtual columns
   * are the ones that are not stored in the tables. For now these are just the partitioning
   * columns.
   */
  private boolean isVirtual;
  
  transient private TypeInfo type;

  public ColumnInfo() {
  }

  public ColumnInfo(String internalName, TypeInfo type) {
    this.internalName = internalName;
    this.type = type;
  }
  
  public ColumnInfo(String internalName, Class type) {
    this.internalName = internalName;
    this.type = TypeInfoFactory.getPrimitiveTypeInfo(type);
  }
  
  public TypeInfo getType() {
    return type;
  }

  public String getInternalName() {
    return internalName;
  }
  
  public void setType(TypeInfo type) {
    this.type = type;
  }

  public void setInternalName(String internalName) {
    this.internalName = internalName;
  }
}
