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
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName="Alter Table")
public class alterTableDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  public static enum alterTableTypes {RENAME, ADDCOLS, REPLACECOLS};
    
  alterTableTypes      op;
  String               oldName;
  String               newName;
  List<FieldSchema>    newCols;
  
  /**
   * @param oldName old name of the table
   * @param newName new name of the table
   */
  public alterTableDesc(String oldName, String newName) {
    op = alterTableTypes.RENAME;
    this.oldName = oldName;
    this.newName = newName;
  }

  /**
   * @param name name of the table
   * @param newCols new columns to be added
   */
  public alterTableDesc(String name, List<FieldSchema> newCols, alterTableTypes alterType) {
    this.op = alterType;
    this.oldName = name;
    this.newCols = newCols;
  }

  /**
   * @return the old name of the table
   */
  @explain(displayName="old name")
  public String getOldName() {
    return oldName;
  }

  /**
   * @param oldName the oldName to set
   */
  public void setOldName(String oldName) {
    this.oldName = oldName;
  }

  /**
   * @return the newName
   */
  @explain(displayName="new name")
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName the newName to set
   */
  public void setNewName(String newName) {
    this.newName = newName;
  }

  /**
   * @return the op
   */
  public alterTableTypes getOp() {
    return op;
  }

  @explain(displayName="type")
  public String getAlterTableTypeString() {
    switch(op) {
    case RENAME:
      return "rename";
    case ADDCOLS:
      return "add columns";
    case REPLACECOLS:
      return "replace columns";
    }
    
    return "unknown";
  }
  /**
   * @param op the op to set
   */
  public void setOp(alterTableTypes op) {
    this.op = op;
  }

  /**
   * @return the newCols
   */
  public List<FieldSchema> getNewCols() {
    return newCols;
  }

  @explain(displayName="new columns")
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(getNewCols());
  }
  /**
   * @param newCols the newCols to set
   */
  public void setNewCols(List<FieldSchema> newCols) {
    this.newCols = newCols;
  }

}
