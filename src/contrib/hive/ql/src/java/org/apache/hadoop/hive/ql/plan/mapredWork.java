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

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName="Map Reduce")
public class mapredWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private String command;
  // map side work
  //   use LinkedHashMap to make sure the iteration order is
  //   deterministic, to ease testing
  private LinkedHashMap<String,ArrayList<String>> pathToAliases;
  
  private LinkedHashMap<String,partitionDesc> pathToPartitionInfo;
  
  private HashMap<String,Operator<? extends Serializable>> aliasToWork;

  // map<->reduce interface
  // schema of the map-reduce 'key' object - this is homogeneous
  private schemaDesc keySchema;

  // schema of the map-reduce 'val' object - this is heterogeneous
  private HashMap<String,schemaDesc> aliasToSchema;

  private Operator<?> reducer;
  
  private Integer numReduceTasks;
  
  private boolean needsTagging;
  private boolean inferNumReducers;

  public mapredWork() { }
  public mapredWork(
    final String command,
    final LinkedHashMap<String,ArrayList<String>> pathToAliases,
    final LinkedHashMap<String,partitionDesc> pathToPartitionInfo,
    final HashMap<String,Operator<? extends Serializable>> aliasToWork,
    final schemaDesc keySchema,
    HashMap<String,schemaDesc> aliasToSchema,
    final Operator<?> reducer,
    final Integer numReduceTasks) {
    this.command = command;
    this.pathToAliases = pathToAliases;
    this.pathToPartitionInfo = pathToPartitionInfo;
    this.aliasToWork = aliasToWork;
    this.keySchema = keySchema;
    this.aliasToSchema = aliasToSchema;
    this.reducer = reducer;
    this.numReduceTasks = numReduceTasks;
  }
  public String getCommand() {
    return this.command;
  }
  public void setCommand(final String command) {
    this.command = command;
  }

  @explain(displayName="Path -> Alias", normalExplain=false)
  public LinkedHashMap<String,ArrayList<String>> getPathToAliases() {
    return this.pathToAliases;
  }
  public void setPathToAliases(final LinkedHashMap<String,ArrayList<String>> pathToAliases) {
    this.pathToAliases = pathToAliases;
  }

  @explain(displayName="Path -> Partition", normalExplain=false)
  public LinkedHashMap<String,partitionDesc> getPathToPartitionInfo() {
    return this.pathToPartitionInfo;
  }
  public void setPathToPartitionInfo(final LinkedHashMap<String,partitionDesc> pathToPartitionInfo) {
    this.pathToPartitionInfo = pathToPartitionInfo;
  }
  
  @explain(displayName="Alias -> Map Operator Tree")
  public HashMap<String, Operator<? extends Serializable>> getAliasToWork() {
    return this.aliasToWork;
  }
  public void setAliasToWork(final HashMap<String,Operator<? extends Serializable>> aliasToWork) {
    this.aliasToWork=aliasToWork;
  }
  public schemaDesc getKeySchema() {
    return this.keySchema;
  }
  public void setKeySchema(final schemaDesc keySchema) {
    this.keySchema = keySchema;
  }
  public HashMap<String,schemaDesc> getAliasToSchema() {
    return this.aliasToSchema;
  }
  public void setAliasToSchema(final HashMap<String,schemaDesc> aliasToSchema) {
    this.aliasToSchema = aliasToSchema;
  }

  @explain(displayName="Reduce Operator Tree")
  public Operator<?> getReducer() {
    return this.reducer;
  }

  public void setReducer(final Operator<?> reducer) {
    this.reducer = reducer;
  }

  @explain(displayName="# Reducers")
  public Integer getNumReduceTasks() {
    return this.numReduceTasks;
  }
  public void setNumReduceTasks(final Integer numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
  }
  @SuppressWarnings("nls")
  public void  addMapWork(String path, String alias, Operator<?> work, partitionDesc pd) {
    ArrayList<String> curAliases = this.pathToAliases.get(path);
    if(curAliases == null) {
      assert(this.pathToPartitionInfo.get(path) == null);
      curAliases = new ArrayList<String> ();
      this.pathToAliases.put(path, curAliases);
      this.pathToPartitionInfo.put(path, pd);
    } else {
      assert(this.pathToPartitionInfo.get(path) != null);
    }

    for(String oneAlias: curAliases) {
      if(oneAlias.equals(alias)) {
        throw new RuntimeException ("Multiple aliases named: " + alias + " for path: " + path);
      }
    }
    curAliases.add(alias);

    if(this.aliasToWork.get(alias) != null) {
      throw new RuntimeException ("Existing work for alias: " + alias);
    }
    this.aliasToWork.put(alias, work);
  }

  @SuppressWarnings("nls")
  public String isInvalid () {
    if((getNumReduceTasks() >= 1) && (getReducer() == null)) {
      return "Reducers > 0 but no reduce operator";
    }

    if((getNumReduceTasks() == 0) && (getReducer() != null)) {
      return "Reducers == 0 but reduce operator specified";
    }

    return null;
  }

  public String toXML () {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Utilities.serializeMapRedWork(this, baos);
    return (baos.toString());
  }

  // non bean

  /**
   * For each map side operator - stores the alias the operator is working on behalf
   * of in the operator runtime state. This is used by reducesink operator - but could
   * be useful for debugging as well.
   */
  private void setAliases () {
    for(String oneAlias: this.aliasToWork.keySet()) {
      this.aliasToWork.get(oneAlias).setAlias(oneAlias);
    }
  }

  public void initialize () {
    setAliases();
  }

  @explain(displayName="Needs Tagging", normalExplain=false)
  public boolean getNeedsTagging() {
    return this.needsTagging;
  }
  
  public void setNeedsTagging(boolean needsTagging) {
    this.needsTagging = needsTagging;
  }

  public boolean getInferNumReducers() {
    return this.inferNumReducers;
  }
  
  public void setInferNumReducers(boolean inferNumReducers) {
    this.inferNumReducers = inferNumReducers;
  }

}
