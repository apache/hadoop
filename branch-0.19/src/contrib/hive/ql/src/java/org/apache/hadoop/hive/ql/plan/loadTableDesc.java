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

public class loadTableDesc extends org.apache.hadoop.hive.ql.plan.loadDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean replace;

  // TODO: the below seems like they should just be combined into partitionDesc
  private org.apache.hadoop.hive.ql.plan.tableDesc table;
  private HashMap<String, String> partitionSpec;

  public loadTableDesc() { }
  public loadTableDesc(
    final String sourceDir,
    final org.apache.hadoop.hive.ql.plan.tableDesc table,
    final HashMap<String, String> partitionSpec,
    final boolean replace) {

    super(sourceDir);
    this.table = table;
    this.partitionSpec = partitionSpec;
    this.replace = replace;
  }
  public loadTableDesc(
    final String sourceDir,
    final org.apache.hadoop.hive.ql.plan.tableDesc table,
    final HashMap<String, String> partitionSpec) {
    this(sourceDir, table, partitionSpec, true);
  }

  @explain(displayName="table")
  public tableDesc getTable() {
    return this.table;
  }
  public void setTable(final org.apache.hadoop.hive.ql.plan.tableDesc table) {
    this.table = table;
  }
  
  @explain(displayName="partition")
  public HashMap<String, String> getPartitionSpec() {
    return this.partitionSpec;
  }
  public void setPartitionSpec(final HashMap<String, String> partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  @explain(displayName="replace")
  public boolean getReplace() {
    return replace;
  }
  public void setReplace(boolean replace) {
    this.replace = replace;
  }
}
