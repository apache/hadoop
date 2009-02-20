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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftMetaStore;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.util.StringUtils;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TTransportException;

/**
 * TODO Unnecessary when the server sides for both dbstore and filestore are merged
 */
public class MetaStoreClient implements IMetaStoreClient {

  private TTransport transport;
  private ThriftMetaStore.Iface client;
  private boolean open;
  private Configuration conf;
  private URI metastoreUris[];
  private Warehouse wh;
  public static final Log LOG = LogFactory.getLog("hive.metastore.client");

  // for thrift connects
  private static final int retries = 5;

  public MetaStoreClient(Configuration configuration) throws MetaException {
    if(configuration == null) {
      configuration = new HiveConf(MetaStoreClient.class);
    }
    
    wh = new Warehouse(configuration);

    if(HiveConf.getVar(configuration, HiveConf.ConfVars.METASTOREURIS) != null) {
      String metastoreUrisString []= HiveConf.getVar(configuration, HiveConf.ConfVars.METASTOREURIS).split(",");
      this.metastoreUris = new URI[metastoreUrisString.length];
      try {
        int i = 0;
        for(String s: metastoreUrisString) {
          URI tmpUri = new URI(s.trim());
          if(tmpUri.getScheme() == null) {
            throw new IllegalArgumentException("URI: "+s+" does not have a scheme");
          }
          this.metastoreUris[i++]= tmpUri;

        }
      } catch (IllegalArgumentException e) {
        throw (e);
      } catch(Exception e) {
        System.err.println("Exception getting uri to connect to the store with: " + e.getMessage());
        e.printStackTrace();
      }
    } else if(HiveConf.getVar(configuration,  HiveConf.ConfVars.METASTOREDIRECTORY) != null) {
      this.metastoreUris = new URI[1];
      try {
        this.metastoreUris[0] = new URI(HiveConf.getVar(configuration,  HiveConf.ConfVars.METASTOREDIRECTORY));
      } catch(URISyntaxException e) {
        System.err.println("Exception getting uri to connect to the store with: " + e.getMessage());
        e.printStackTrace();
      }
    } else {
      System.err.println("NOT getting uris from conf");
      throw new RuntimeException("MetaStoreURIs not found in conf file");
    }
    this.conf = configuration;
    transport = null;
    client = null;
    open = false;
  }

  public void open(URI store) throws TException {
    this.open = false;
    if(store.getScheme().equals("thrift")) {
      transport = new TSocket(store.getHost(), store.getPort());
      ((TSocket)transport).setTimeout(2000);
      TProtocol protocol = new TBinaryProtocol(transport);
      client = new ThriftMetaStore.Client(protocol);

      for(int i = 0; i < retries && !this.open; ++i) {
        try {
          transport.open();
          this.open = true;
        } catch(TTransportException e) {
          System.err.println("WARN: failed to connect to MetaStore, re-trying...");
          try {
            Thread.sleep(1000);
          } catch(InterruptedException ignore) { }
        }
      }
      if(!open) {
        throw new TException("could not connect to meta store");
      }
    } else if(store.getScheme().equals("file")) {
      client = new MetaStoreServer.ThriftMetaStoreHandler("temp_server", this.conf);
      try {
        // for some reason setOption in FB303 doesn't allow one to throw a TException,
        // so I'm having it throw a RuntimeException since that doesn't require changing
        // the method signature.
        client.setOption("metastore.path",store.toASCIIString());
      } catch(RuntimeException e) {
        System.err.println("Could not setoption metastore.path to " + store.getPath());
        throw new TException("could not set metastore path to: " + store.getPath());
      }
    } else {
      throw new TException("Unknown scheme to connect to MetaStore: " + store.getScheme());
    }
  }

  public void close() {
    if(this.open) {
      transport.close();
    }
  }

  public List<String> getTables(String dbName, String tablePattern) throws MetaException, UnknownTableException, TException, UnknownDBException  {

    TException firstException = null;
    for(URI store: this.metastoreUris) {
      try {
        this.open(store);
        List<String> ret = client.get_tables(dbName, tablePattern);
        this.close();
        return ret;
      }  catch(TException e) {
        System.err.println("get_tables got exception: " + e);
        if(firstException == null) {
          firstException = e;
        }
      }
      // note - do not catchMetaException or UnkownTableException as we want those to propagate up w/o retrying backup stores.
    }
    throw firstException;
  }

  public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException  {
    TException firstException = null;
    String dbName = "default";
    // Ignore deleteData for now
    for(URI store: this.metastoreUris) {
      try {
        this.open(store);
        try {
          client.drop_table(dbName, tableName);
        } catch(UnknownDBException e) {
          throw new UnknownTableException();
        }
        close();
        return;
      }  catch(TException e) {
        System.err.println("get_tables got exception: " + e);
        if(firstException == null) {
          firstException = e;
        }
      }
      // note - do not catchMetaException or UnkownTableException as we want those to propagate up w/o retrying backup stores.
    }
    if(firstException != null) {
      throw firstException;
    }
  }

  public void createTable(String tableName, Properties schema) throws MetaException, UnknownTableException, TException  {

    TException firstException = null;
    String dbName = "default";
    HashMap<String, String> hm = new HashMap<String, String> ();
    for(Enumeration<?> e = schema.propertyNames();
        e.hasMoreElements() ; ) {
      String key = (String)e.nextElement();
      String val = schema.getProperty(key);
      hm.put(key, val);
    }
    for(URI store: this.metastoreUris) {
      try {
        this.open(store);
        try {
          client.create_table(dbName, tableName, hm);
        } catch(UnknownDBException e) {
          throw new UnknownTableException();
        }
        close();
        return;
      } catch(TException e) {
        System.err.println("get_tables got exception: " + e);
        if(firstException == null) {
          firstException = e;
        }
      }
      // note - do not catchMetaException or UnkownTableException as we want those to propagate up w/o retrying backup stores.
    }
    if(firstException != null) {
      throw firstException;
    }
  }


  public Properties getSchema(String tableName) throws MetaException, TException, NoSuchObjectException {
    TException firstException = null;
    for(URI store: this.metastoreUris) {
      try {
        this.open(store);
        Map<String, String> schema_map = client.get_schema(tableName);
        Properties p = new Properties();
        for(Iterator it = schema_map.entrySet().iterator(); it.hasNext() ; ) {
          Map.Entry e = (Map.Entry)it.next();
          String key = (String)e.getKey();
          String val = (String)e.getValue();
          p.setProperty(key,val);
        }
        p = MetaStoreUtils.hive1Tohive3ClassNames(p);
        this.close();
        return p;
      } catch(TException e) {
        System.err.println("get_schema got exception: " + e);
        e.printStackTrace();
        if(firstException == null) {
          firstException = e;
        }
      } catch (UnknownTableException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new NoSuchObjectException(e.getMessage());
      } catch (UnknownDBException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new NoSuchObjectException(e.getMessage());
      }
    }
    throw firstException;
  }

  public boolean tableExists(String tableName) throws MetaException,TException, UnknownDBException {
    TException firstException = null;
    String dbName = "default";
    for(URI store: this.metastoreUris) {
      try {
        this.open(store);
        boolean ret = client.table_exists(dbName, tableName);
        this.close();
        return ret;
      }  catch(TException e) {
        System.err.println("schema_exists got exception: " + e);
        if(firstException == null) {
          firstException = e;
        }
      }
      // note - do not catchMetaException or UnkownTableException as we want those to propagate up w/o retrying backup stores.
    }
    throw firstException;
  }

  public List<FieldSchema> get_fields(String tableName) throws MetaException,UnknownTableException, TException {
    TException firstException = null;
    String dbName = "default";
    for(URI store: this.metastoreUris) {
      try {
        this.open(store);
        List<FieldSchema> fields = client.get_fields(dbName, tableName);
        this.close();
        return fields;
      }  catch(UnknownDBException e) {
        throw new MetaException(e.getMessage());
      }  catch(TException e) {
        System.err.println("get_schema got exception: " + e);
        e.printStackTrace();
        if(firstException == null) {
          firstException = e;
        }
      }
      // note - do not catchMetaException or UnkownTableException as we want those to propagate up w/o retrying backup stores.
    }
    throw firstException;
  }

  public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
    Properties schema = this.getSchema(tableName);
    return MetaStoreUtils.getTable(conf, schema);
  }

  //These will disappear when the server is unified for both filestore and dbstore
  @Override
  public List<Partition> listPartitions(String dbName, String tableName, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    //TODO: move the code from Table.getPartitions() to here
    return new ArrayList<Partition>();
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, TException {
    if(partVals.size() == 0) {
      return null;
    }
    try {
      // need to get the table for partition key names.
      // this is inefficient because caller of this function has table already
      Partition part = getPartitionObject(dbName, tableName, partVals);
      if(!wh.isDir(new Path(part.getSd().getLocation()))) {
        // partition doesn't exist in hdfs so return nothing
        return null;
      }
      return part;
    } catch (NoSuchObjectException e) {
      return null;
    }
  }

  @Override
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    Properties schema = MetaStoreUtils.getSchema(tbl);
    try {
      this.createTable(tbl.getTableName(), schema);
    } catch (UnknownTableException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new NoSuchObjectException(e.getMessage());
    }
  }

  public Partition appendPartition(String dbName, String tableName, List<String> partVals)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if(partVals.size() == 0) {
      return null;
    }
    Partition part = null;
    try {
      // need to get the table for partition key names.
      // this is inefficient because caller of this function has table already
      part = getPartitionObject(dbName, tableName, partVals);
      wh.mkdirs(new Path(part.getSd().getLocation())); // this will throw an exception if the dir couldn't be created
      return part;
    } catch (NoSuchObjectException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new InvalidObjectException("table or database doesn't exist");
    }
  }

  /**
   * @param tableName
   * @param dbName
   * @param partVals
   * @return
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException
   */
  private Partition getPartitionObject(String dbName, String tableName, List<String> partVals)
      throws MetaException, TException, NoSuchObjectException {
    Properties schema = this.getSchema(tableName);
    Table tbl = MetaStoreUtils.getTable(conf, schema);
    List<FieldSchema> partKeys = tbl.getPartitionKeys();
    if(partKeys.size() != partVals.size()) {
      throw new MetaException("Invalid partition key values: " + partVals);
    }
    LinkedHashMap<String, String> pm = new LinkedHashMap<String, String>(partKeys.size());
    for (int i=0; i < partKeys.size(); i++) {
      if(partVals.get(i) == null || partVals.get(i).length() == 0) {
        throw new MetaException("Invalid partition spec: " + partVals);
      }
      pm.put(partKeys.get(i).getName(), partVals.get(i));
    }
    Path partPath = wh.getPartitionPath(dbName, tableName, pm);
    Partition tPartition = new Partition();
    tPartition.setValues(partVals);
    tPartition.setSd(tbl.getSd()); // TODO: get a copy
    tPartition.setParameters(new HashMap<String, String>());
    tPartition.getSd().setLocation(partPath.toString());
    return tPartition;
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {
    throw new MetaException("Not yet implementd in filestore");
  }

  @Override
  public boolean createDatabase(String name, String location_uri) throws AlreadyExistsException,
      MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dropDatabase(String name) throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws MetaException, TException {
    // TODO Auto-generated method stub
    return new ArrayList<String>();
  }

  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return false;
  }

}
