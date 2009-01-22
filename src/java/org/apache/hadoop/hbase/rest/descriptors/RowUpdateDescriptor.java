/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rest.descriptors;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class RowUpdateDescriptor {
  private String tableName;
  private String rowName;
  private Map<byte[], byte[]> colvals = new HashMap<byte[], byte[]>();
  
  public RowUpdateDescriptor(String tableName, String rowName) {
    this.tableName = tableName;
    this.rowName = rowName;
  }
  
  public RowUpdateDescriptor() {}

  /**
   * @return the tableName
   */
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
   * @return the rowName
   */
  public String getRowName() {
    return rowName;
  }

  /**
   * @param rowName the rowName to set
   */
  public void setRowName(String rowName) {
    this.rowName = rowName;
  }

  /**
   * @return the test
   */
  public Map<byte[], byte[]> getColVals() {
    return colvals;
  }  
}
