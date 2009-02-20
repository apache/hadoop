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

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;

import com.facebook.thrift.TException;

/**
 * TODO Unnecessary when the server sides for both dbstore and filestore are merged
 */
public interface IMetaStoreClient {

  public void close();

  public List<String> getTables(String dbName, String tablePattern) throws MetaException, UnknownTableException, TException,
      UnknownDBException;

  public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException, NoSuchObjectException;

  //public void createTable(String tableName, Properties schema) throws MetaException, UnknownTableException,
    //  TException;

  public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException;

  public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException;

  /**
   * @param table_name
   * @param db_name
   * @param part_vals
   * @return
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String, java.lang.String, java.util.List)
   */
  public Partition appendPartition(String tableName, String dbName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#lookup_partition(java.lang.String, java.lang.String, java.util.List)
   */
  public Partition getPartition(String tblName, String dbName, List<String> partVals)
      throws MetaException, TException ;
  
  /**
   * @param tbl_name
   * @param db_name
   * @param max_parts
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#list_partitions(java.lang.String, java.lang.String, short)
   */
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException;

  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
    throws  MetaException, TException;
  /**
   * @param tbl
   * @return
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException ;

  public void alter_table(String defaultDatabaseName, String tblName, Table table) throws InvalidOperationException, MetaException, TException;
  public boolean createDatabase(String name, String location_uri) throws AlreadyExistsException, MetaException, TException;
  public boolean dropDatabase(String name) throws MetaException, TException;

  /**
   * @param db_name
   * @param tbl_name
   * @param part_vals
   * @param deleteData delete the underlying data or just delete the table in metadata
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String, java.lang.String, java.util.List)
   */
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException;
}