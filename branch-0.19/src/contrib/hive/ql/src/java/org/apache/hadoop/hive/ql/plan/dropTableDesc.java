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
import java.util.HashMap;
import java.util.List;

@explain(displayName="Drop Table")
public class dropTableDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  
  String            tableName;
  List<HashMap<String, String>> partSpecs;

  /**
   * @param tableName
   */
  public dropTableDesc(String tableName) {
    this.tableName = tableName;
    this.partSpecs = null;
  }

  public dropTableDesc(String tableName, List<HashMap<String, String>> partSpecs) {
    this.tableName = tableName;
    this.partSpecs = partSpecs;
  }

  /**
   * @return the tableName
   */
  @explain(displayName="table")
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the partSpecs
   */
  public List<HashMap<String, String>> getPartSpecs() {
    return partSpecs;
  }

  /**
   * @param partSpecs the partSpecs to set
   */
  public void setPartSpecs(List<HashMap<String, String>> partSpecs) {
    this.partSpecs = partSpecs;
  }
}
