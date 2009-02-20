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

public class MPartition {

  private String partitionName; // partitionname ==>  (key=value/)*(key=value)
  private MTable table; 
  private List<String> values;
  private int createTime;
  private int lastAccessTime;
  private MStorageDescriptor sd;
  private Map<String, String> parameters;
  
  
  public MPartition() {}
  
  /**
   * @param partitionName
   * @param table
   * @param values
   * @param createTime
   * @param lastAccessTime
   * @param sd
   * @param parameters
   */
  public MPartition(String partitionName, MTable table, List<String> values, int createTime,
      int lastAccessTime, MStorageDescriptor sd, Map<String, String> parameters) {
    this.partitionName = partitionName;
    this.table = table;
    this.values = values;
    this.createTime = createTime;
    this.lastAccessTime = lastAccessTime;
    this.sd = sd;
    this.parameters = parameters;
  }

  /**
   * @return the lastAccessTime
   */
  public int getLastAccessTime() {
    return lastAccessTime;
  }

  /**
   * @param lastAccessTime the lastAccessTime to set
   */
  public void setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  /**
   * @return the values
   */
  public List<String> getValues() {
    return values;
  }

  /**
   * @param values the values to set
   */
  public void setValues(List<String> values) {
    this.values = values;
  }

  /**
   * @return the table
   */
  public MTable getTable() {
    return table;
  }

  /**
   * @param table the table to set
   */
  public void setTable(MTable table) {
    this.table = table;
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
   * @return the partitionName
   */
  public String getPartitionName() {
    return partitionName;
  }

  /**
   * @param partitionName the partitionName to set
   */
  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
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

}
