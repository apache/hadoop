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

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;

public interface RawStore extends Configurable {

  public abstract void shutdown();

  /**
   * Opens a new one or the one already created
   * Every call of this function must have corresponding commit or rollback function call
   * @return an active transaction
   */

  public abstract boolean openTransaction();

  /**
   * if this is the commit of the first open call then an actual commit is called. 
   * @return 
   */
  public abstract boolean commitTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
  public abstract void rollbackTransaction();

  public abstract boolean createDatabase(Database db) throws MetaException;

  public abstract boolean createDatabase(String name) throws MetaException;

  public abstract Database getDatabase(String name) throws NoSuchObjectException;

  public abstract boolean dropDatabase(String dbname);

  public abstract List<String> getDatabases() throws MetaException;

  public abstract boolean createType(Type type);

  public abstract Type getType(String typeName);

  public abstract boolean dropType(String typeName);

  public abstract void createTable(Table tbl) throws InvalidObjectException, MetaException;

  public abstract boolean dropTable(String dbName, String tableName) throws MetaException;

  public abstract Table getTable(String dbName, String tableName) throws MetaException;

  public abstract boolean addPartition(Partition part) throws InvalidObjectException, MetaException;

  public abstract Partition getPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException;

  public abstract boolean dropPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException;

  public abstract List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException;

  // TODO: add tests
  public abstract void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  public List<String> getTables(String dbName, String pattern) throws MetaException;

  public abstract List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws MetaException;
}
