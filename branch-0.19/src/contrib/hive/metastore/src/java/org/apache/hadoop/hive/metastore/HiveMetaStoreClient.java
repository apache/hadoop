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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ExistingDependentsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TTransportException;

/**
 * Hive Metastore Client.
 */
public class HiveMetaStoreClient implements IMetaStoreClient {
  ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean open = false;
  private URI metastoreUris[];
  private boolean standAloneClient = false;

  // for thrift connects
  private int retries = 5;

  static final private Log LOG = LogFactory.getLog("hive.metastore");


  public HiveMetaStoreClient(HiveConf conf) throws MetaException {
    if(conf == null) {
      conf = new HiveConf(HiveMetaStoreClient.class);
    }
    
    boolean localMetaStore = conf.getBoolean("hive.metastore.local", false);
    if(localMetaStore) {
      // instantiate the metastore server handler directly instead of connecting through the network
      client = new HiveMetaStore.HMSHandler("hive client", conf);
      this.open = true;
      return;
    }
    
    // get the number retries
    retries = conf.getInt("hive.metastore.connect.retries", 5);

    // user wants file store based configuration
    if(conf.getVar(HiveConf.ConfVars.METASTOREURIS) != null) {
      String metastoreUrisString []= conf.getVar(HiveConf.ConfVars.METASTOREURIS).split(",");
      this.metastoreUris = new URI[metastoreUrisString.length];
      try {
        int i = 0;
        for(String s: metastoreUrisString) {
          URI tmpUri = new URI(s);
          if(tmpUri.getScheme() == null) {
            throw new IllegalArgumentException("URI: "+s+" does not have a scheme");
          }
          this.metastoreUris[i++]= tmpUri;

        }
      } catch (IllegalArgumentException e) {
        throw (e);
      } catch(Exception e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else if(conf.getVar(HiveConf.ConfVars.METASTOREDIRECTORY) != null) {
      this.metastoreUris = new URI[1];
      try {
        this.metastoreUris[0] = new URI(conf.getVar(HiveConf.ConfVars.METASTOREDIRECTORY));
      } catch(URISyntaxException e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else {
      LOG.error("NOT getting uris from conf");
      throw new MetaException("MetaStoreURIs not found in conf file");
    }
    // finally open the store
    this.open();
  }
  
  /**
   * @param dbname
   * @param tbl_name
   * @param new_tbl
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_table(java.lang.String, java.lang.String, org.apache.hadoop.hive.metastore.api.Table)
   */
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {
    client.alter_table(dbname, tbl_name, new_tbl);
  }

  private void open() throws MetaException {
    for(URI store: this.metastoreUris) {
      LOG.info("Trying to connect to metastore with URI " + store);
      try {
        openStore(store);
      } catch (MetaException e) {
        LOG.warn(e.getStackTrace());
        LOG.warn("Unable to connect metastore with URI " + store);
      }
      if (open) {
        break;
      }
    }
    if(!open) {
      throw new MetaException("Could not connect to meta store using any of the URIs provided");
    }
    LOG.info("Connected to metastore.");
  }
 
  private void openStore(URI store) throws MetaException {
    open = false;
    transport = new TSocket(store.getHost(), store.getPort());
    ((TSocket)transport).setTimeout(20000);
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new ThriftHiveMetastore.Client(protocol);

    for(int i = 0; i < retries && !this.open; ++i) {
      try {
        transport.open();
        open = true;
      } catch(TTransportException e) {
        LOG.warn("failed to connect to MetaStore, re-trying...");
        try {
          Thread.sleep(1000);
        } catch(InterruptedException ignore) { }
      }
    }
    if(!open) {
      throw new MetaException("could not connect to meta store");
    }
  }
  
  public void close() {
    open = false;
    if((transport != null) && transport.isOpen()) {
      transport.close();
    }
    if(standAloneClient) {
      try {
        client.shutdown();
      } catch (TException e) {
        //TODO:pc cleanup the exceptions
        LOG.error("Unable to shutdown local metastore client");
        LOG.error(e.getStackTrace());
        //throw new RuntimeException(e.getMessage());
      }
    }
  }

  public void dropTable(String tableName, boolean deleteData) throws MetaException, NoSuchObjectException {
    // assume that it is default database
    try {
      this.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, deleteData, false);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
  }
  
  /**
   * @param new_part
   * @return
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partition(org.apache.hadoop.hive.metastore.api.Partition)
   */
  public Partition add_partition(Partition new_part) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    return client.add_partition(new_part);
  }

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
  public Partition appendPartition(String db_name, String table_name, List<String> part_vals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.append_partition(db_name, table_name, part_vals);
  }

  /**
   * @param name
   * @param location_uri
   * @return
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_database(java.lang.String, java.lang.String)
   */
  public boolean createDatabase(String name, String location_uri) throws AlreadyExistsException,
      MetaException, TException {
    return client.create_database(name, location_uri);
  }

  /**
   * @param tbl
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException,
  MetaException, NoSuchObjectException, TException {
    client.create_table(tbl);
  }

  /**
   * @param type
   * @return
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_type(org.apache.hadoop.hive.metastore.api.Type)
   */
  public boolean createType(Type type) throws AlreadyExistsException, InvalidObjectException,
      MetaException, TException {
    return client.create_type(type);
  }

  /**
   * @param name
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String)
   */
  public boolean dropDatabase(String name) throws MetaException, TException {
    return client.drop_database(name);
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String, java.lang.String, java.util.List)
   */
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals)
      throws NoSuchObjectException, MetaException, TException {
        return dropPartition(db_name, tbl_name, part_vals, true);
      }

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
      throws NoSuchObjectException, MetaException, TException {
    return client.drop_partition(db_name, tbl_name, part_vals, deleteData);
  }
  
  /**
   * @param name
   * @param dbname
   * @return
   * @throws NoSuchObjectException
   * @throws ExistingDependentsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String, java.lang.String)
   */
  public void dropTable(String dbname, String name) throws NoSuchObjectException,
      ExistingDependentsException, MetaException, TException {
        dropTable(dbname, name, true, true);
      }

  /**
   * @param dbname
   * @param name
   * @param deleteData delete the underlying data or just delete the table in metadata
   * @return
   * @throws NoSuchObjectException
   * @throws ExistingDependentsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String, java.lang.String)
   */
  public void dropTable(String dbname, String name, boolean deleteData, boolean ignoreUknownTab) throws 
      ExistingDependentsException, MetaException, TException, NoSuchObjectException {
    try {
      client.drop_table(dbname, name, deleteData);
    } catch (NoSuchObjectException e) {
      if(!ignoreUknownTab) {
        throw e;
      }
    }
  }

  /**
   * @param type
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_type(java.lang.String)
   */
  public boolean dropType(String type) throws MetaException, TException {
    return client.drop_type(type);
  }

  /**
   * @param name
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type_all(java.lang.String)
   */
  public Map<String, Type> getTypeAll(String name) throws MetaException, TException {
    return client.get_type_all(name);
  }

  /**
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#list_databases()
   */
  public List<String> getDatabases() throws MetaException, TException {
    return client.get_databases();
  }

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
      throws NoSuchObjectException, MetaException, TException {
    return client.get_partitions(db_name, tbl_name, max_parts);
  }

  /**
   * @param name
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#lookup_database(java.lang.String)
   */
  public Database getDatabase(String name) throws NoSuchObjectException, MetaException,
      TException {
    return client.get_database(name);
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#lookup_partition(java.lang.String, java.lang.String, java.util.List)
   */
  public Partition getPartition(String db_name, String tbl_name, List<String> part_vals)
      throws MetaException, TException {
    return client.get_partition(db_name, tbl_name, part_vals);
  }
  
  /**
   * @param name
   * @param dbname
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException 
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#lookup_table(java.lang.String, java.lang.String)
   */
  public Table getTable(String dbname, String name) throws MetaException, TException, NoSuchObjectException {
    return client.get_table(dbname, name);
  }

  /**
   * @param name
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#lookup_type(java.lang.String)
   */
  public Type getType(String name) throws MetaException, TException {
    return client.get_type(name);
  }

  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return client.get_tables(dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null; 
  }
  
  public List<String> getTables(String tablePattern) throws MetaException {
    String dbname = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    return this.getTables(dbname, tablePattern); 
  }

  public boolean tableExists(String tableName) throws MetaException, TException,
  UnknownDBException {
    try {
      client.get_table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
    return getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short max)
      throws MetaException, TException {
    // TODO Auto-generated method stub
    return client.get_partition_names(dbName, tblName, max);
  }

}
