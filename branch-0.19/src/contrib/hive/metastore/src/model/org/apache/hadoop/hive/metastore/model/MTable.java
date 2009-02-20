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

package org.apache.hadoop.hive.metastore.model;

import java.util.List;
import java.util.Map;

public class MTable {
  
  private String tableName;
  private MDatabase database;
  private MStorageDescriptor sd;
  private String owner;
  private int createTime;
  private int lastAccessTime;
  private int retention;
  private List<MFieldSchema> partitionKeys;
  private Map<String, String> parameters;
  
  public MTable() {}

  /**
   * @param tableName
   * @param database
   * @param sd
   * @param owner
   * @param createTime
   * @param lastAccessTime
   * @param retention
   * @param partitionKeys
   * @param parameters
   */
  public MTable(String tableName, MDatabase database, MStorageDescriptor sd, String owner,
      int createTime, int lastAccessTime, int retention, List<MFieldSchema> partitionKeys,
      Map<String, String> parameters) {
    this.tableName = tableName;
    this.database = database;
    this.sd = sd;
    this.owner = owner;
    this.createTime = createTime;
    this.setLastAccessTime(lastAccessTime);
    this.retention = retention;
    this.partitionKeys = partitionKeys;
    this.parameters = parameters;
  }

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
   * @return the sd
   */
  public MStorageDescriptor getSd() {
    return sd;
  }

  /**
   * @param sd the sd to set
   */
  public void setSd(MStorageDescriptor sd) {
    this.sd = sd;
  }

  /**
   * @return the partKeys
   */
  public List<MFieldSchema> getPartitionKeys() {
    return partitionKeys;
  }

  /**
   * @param partKeys the partKeys to set
   */
  public void setPartitionKeys(List<MFieldSchema> partKeys) {
    this.partitionKeys = partKeys;
  }

  /**
   * @return the parameters
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return owner;
  }

  /**
   * @param owner the owner to set
   */
  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * @return the createTime
   */
  public int getCreateTime() {
    return createTime;
  }

  /**
   * @param createTime the createTime to set
   */
  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  /**
   * @return the database
   */
  public MDatabase getDatabase() {
    return database;
  }

  /**
   * @param database the database to set
   */
  public void setDatabase(MDatabase database) {
    this.database = database;
  }

  /**
   * @return the retention
   */
  public int getRetention() {
    return retention;
  }

  /**
   * @param retention the retention to set
   */
  public void setRetention(int retention) {
    this.retention = retention;
  }

  /**
   * @param lastAccessTime the lastAccessTime to set
   */
  public void setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  /**
   * @return the lastAccessTime
   */
  public int getLastAccessTime() {
    return lastAccessTime;
  }

  
}
