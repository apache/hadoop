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

import java.util.*;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of the metadata information related to a query block
 *
 **/

public class QBMetaData {

  public static final int DEST_INVALID = 0;
  public static final int DEST_TABLE = 1;
  public static final int DEST_PARTITION = 2;
  public static final int DEST_DFS_FILE = 3;
  public static final int DEST_REDUCE = 4;
  public static final int DEST_LOCAL_FILE = 5;

  private ArrayList<Class<?>> outTypes;
  private HashMap<String, Table> aliasToTable;
  private HashMap<String, Table> nameToDestTable;
  private HashMap<String, Partition> nameToDestPartition;
  private HashMap<String, String> nameToDestFile;
  private HashMap<String, Integer> nameToDestType;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBMetaData.class.getName());
  
  public QBMetaData() {
    this.outTypes = new ArrayList<Class<?>>();
    this.aliasToTable = new HashMap<String, Table>();
    this.nameToDestTable = new HashMap<String, Table>();
    this.nameToDestPartition = new HashMap<String, Partition>();
    this.nameToDestFile = new HashMap<String, String>();
    this.nameToDestType = new HashMap<String, Integer>();
  }

  public ArrayList<Class<?>> getOutputTypes() {
    return this.outTypes;
  }

  public void addOutputType(Class<?> cls) {
    this.outTypes.add(cls);
  }

  // All getXXX needs toLowerCase() because they are directly called from SemanticAnalyzer
  // All setXXX does not need it because they are called from QB which already lowercases
  // the aliases.

  public HashMap<String, Table> getAliasToTable() {
    return aliasToTable;
  }

  public Table getTableForAlias(String alias) {
    return this.aliasToTable.get(alias.toLowerCase());
  }

  public void setSrcForAlias(String alias, Table tab) {
    this.aliasToTable.put(alias, tab);
  }

  public void setDestForAlias(String alias, Table tab) {
    this.nameToDestType.put(alias, Integer.valueOf(DEST_TABLE));
    this.nameToDestTable.put(alias, tab);
  }

  public void setDestForAlias(String alias, Partition part) {
    this.nameToDestType.put(alias, Integer.valueOf(DEST_PARTITION));
    this.nameToDestPartition.put(alias, part);
  }

  public void setDestForAlias(String alias, String fname, boolean isDfsFile) {
    this.nameToDestType.put(alias, 
                       isDfsFile ? Integer.valueOf(DEST_DFS_FILE) : Integer.valueOf(DEST_LOCAL_FILE));
    this.nameToDestFile.put(alias, fname);
  }

  public Integer getDestTypeForAlias(String alias) {
    return this.nameToDestType.get(alias.toLowerCase());
  }

  public Table getDestTableForAlias(String alias) {
    return this.nameToDestTable.get(alias.toLowerCase());
  }

  public Partition getDestPartitionForAlias(String alias) {
    return this.nameToDestPartition.get(alias.toLowerCase());
  }

  public String getDestFileForAlias(String alias) {
    return this.nameToDestFile.get(alias.toLowerCase());
  }

  public Table getSrcForAlias(String alias) {
    return this.aliasToTable.get(alias.toLowerCase());
  }
  
}
