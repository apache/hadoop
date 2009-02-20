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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.IndexAlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;
import com.facebook.thrift.transport.TTransportFactory;

/**
 * TODO:pc remove application logic to a separate interface. 
 */
public class HiveMetaStore extends ThriftHiveMetastore {
  
    public static class HMSHandler extends FacebookBase implements ThriftHiveMetastore.Iface{
      public static final Log LOG = LogFactory.getLog(HiveMetaStore.class.getName());
      private static boolean createDefaultDB = false;
      private String rawStoreClassName;
      private HiveConf hiveConf; // stores datastore (jpox) properties, right now they come from jpox.properties
      private Warehouse wh; // hdfs warehouse
      private ThreadLocal<RawStore> threadLocalMS = new ThreadLocal() {
        protected synchronized Object initialValue() {
            return null;
        }
      };

      // The next serial number to be assigned
      private boolean checkForDefaultDb;
      private static int nextSerialNum = 0;
      private static ThreadLocal<Integer> threadLocalId = new ThreadLocal() {
        protected synchronized Object initialValue() {
          return new Integer(nextSerialNum++);
        }
      };
      public static Integer get() {
        return threadLocalId.get();     
      }
      
      public HMSHandler(String name) throws MetaException {
        super(name);
        hiveConf = new HiveConf(this.getClass());
        init();
      }
      
      public HMSHandler(String name, HiveConf conf) throws MetaException {
        super(name);
        hiveConf = conf;
        init();
      }

      private ClassLoader classLoader;
      {
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
          classLoader = Configuration.class.getClassLoader();
        }
      }
      
      private boolean init() throws MetaException {
        rawStoreClassName = hiveConf.get("hive.metastore.rawstore.impl");
        checkForDefaultDb = hiveConf.getBoolean("hive.metastore.checkForDefaultDb", true);
        wh = new Warehouse(hiveConf);
        createDefaultDB();
        return true;
      }

      /**
       * @return
       * @throws MetaException 
       */
      private RawStore getMS() throws MetaException {
        RawStore ms = threadLocalMS.get();
        if(ms == null) {
          LOG.info(threadLocalId.get() + ": Opening raw store with implemenation class:" + rawStoreClassName);
          ms = (RawStore) ReflectionUtils.newInstance(getClass(rawStoreClassName, RawStore.class), hiveConf);
          threadLocalMS.set(ms);
          ms = threadLocalMS.get();
        }
        return ms;
      }

      /**
       * create default database if it doesn't exist
       * @throws MetaException
       */
      private void createDefaultDB() throws MetaException {
        if(HMSHandler.createDefaultDB || !checkForDefaultDb) {
          return;
        }
        try {
          getMS().getDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
        } catch (NoSuchObjectException e) {
          getMS().createDatabase(new Database(MetaStoreUtils.DEFAULT_DATABASE_NAME, 
                    wh.getDefaultDatabasePath(MetaStoreUtils.DEFAULT_DATABASE_NAME).toString()));
        }
        HMSHandler.createDefaultDB = true;
      }

      private Class<?> getClass(String rawStoreClassName, Class<RawStore> class1) throws MetaException {
        try {
          return Class.forName(rawStoreClassName, true, classLoader);
        } catch (ClassNotFoundException e) {
          throw new MetaException(rawStoreClassName + " class not found");
        }
      }
      
      private void logStartFunction(String m) {
        LOG.info(threadLocalId.get().toString() + ": " + m);
      }

      private void logStartFunction(String f, String db, String tbl) {
        LOG.info(threadLocalId.get().toString() + ": " + f + " : db=" + db + " tbl=" + tbl);
      }
      
      @Override
      public int getStatus() {
        return fb_status.ALIVE;
      }
      
      public void shutdown() {
        logStartFunction("Shutting down the object store...");
        try {
          if(threadLocalMS.get() != null) {
            getMS().shutdown();
          }
        } catch (MetaException e) {
          LOG.error("unable to shutdown metastore", e);
        }
        System.exit(0);
      }

      public boolean create_database(String name, String location_uri)
      throws AlreadyExistsException, MetaException {
        this.incrementCounter("create_database");
        logStartFunction("create_database: " + name);
        boolean success = false;
        try {
          getMS().openTransaction();
          Database db = new Database(name, location_uri);
          if(getMS().createDatabase(db) && wh.mkdirs(wh.getDefaultDatabasePath(name))) {
            success = getMS().commitTransaction();
          }
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          }
        }
        return success;
      }

      public Database get_database(String name) throws NoSuchObjectException, MetaException {
        this.incrementCounter("get_database");
        logStartFunction("get_database: " + name);
        return getMS().getDatabase(name);
      }

      public boolean drop_database(String name) throws MetaException {
        this.incrementCounter("drop_database");
        logStartFunction("drop_database: " + name);
        if(name.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
          throw new MetaException("Can't drop default database");
        }
        boolean success = false;
        try {
          getMS().openTransaction();
          if(getMS().dropDatabase(name)) {
            success = getMS().commitTransaction();
          }
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          } else {
            wh.deleteDir(wh.getDefaultDatabasePath(name), true);
            // it is not a terrible thing even if the data is not deleted
          }
        }
        return success;
      }

      public List<String> get_databases() throws MetaException {
        this.incrementCounter("get_databases");
        logStartFunction("get_databases");
        return getMS().getDatabases();
      }

      public boolean create_type(Type type) throws AlreadyExistsException, MetaException, InvalidObjectException {
        this.incrementCounter("create_type");
        logStartFunction("create_type: " + type.getName());
        // check whether type already exists
        if(get_type(type.getName()) != null) {
          throw new AlreadyExistsException("Type " + type.getName() + " already exists");
        }

        //TODO:pc Validation of types should be done by clients or here????
        return getMS().createType(type);
      }

      public Type get_type(String name) throws MetaException {
        this.incrementCounter("get_type");
        logStartFunction("get_type: " + name);
        return getMS().getType(name);
      }

      public boolean drop_type(String name) throws MetaException {
        this.incrementCounter("drop_type");
        logStartFunction("drop_type: " + name);
        // TODO:pc validate that there are no types that refer to this 
        return getMS().dropType(name);
      }

      public Map<String, Type> get_type_all(String name) throws MetaException {
        this.incrementCounter("get_type_all");
        // TODO Auto-generated method stub
        logStartFunction("get_type_all");
        throw new MetaException("Not yet implemented");
      }

      public void create_table(Table tbl) throws AlreadyExistsException, MetaException, InvalidObjectException {
        this.incrementCounter("create_table");
        logStartFunction("create_table: db=" + tbl.getDbName() + " tbl=" + tbl.getTableName());
        boolean success = false;
        if(!MetaStoreUtils.validateName(tbl.getTableName())) {
          throw new InvalidObjectException(tbl.getTableName() + " is not a valid object name");
        }
        try {
          getMS().openTransaction();
          Path tblPath = null;
          if(tbl.getSd().getLocation() == null || tbl.getSd().getLocation().isEmpty()) {
            tblPath = wh.getDefaultTablePath(tbl.getDbName(), tbl.getTableName());
            tbl.getSd().setLocation(tblPath.toString());
          } else {
            tblPath = new Path(tbl.getSd().getLocation());
          }
          // get_table checks whether database exists, it should be moved here
          if(is_table_exists(tbl.getDbName(), tbl.getTableName())) {
            throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists");
          }
          getMS().createTable(tbl);
          if(wh.mkdirs(tblPath)) {
            success = getMS().commitTransaction();
          }
      
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          }
        }
      }
      
      public boolean is_table_exists(String dbname, String name) throws MetaException {
        try {
          return (get_table(dbname, name) != null);
        } catch (NoSuchObjectException e) {
          return false;
        }
      }
      
      public void drop_table(String dbname, String name, boolean deleteData) throws NoSuchObjectException, MetaException {
        this.incrementCounter("drop_table");
        logStartFunction("drop_table", dbname, name);
        boolean success = false;
        Path tblPath = null;
        try {
          getMS().openTransaction();
          // drop any partitions
          Table tbl = get_table(dbname, name);
          if (tbl == null) {
            throw new NoSuchObjectException(name + " doesn't exist");
          }
          if(tbl.getSd() == null  || tbl.getSd().getLocation() == null) {
            throw new MetaException("Table metadata is corrupted");
          }
          if(!getMS().dropTable(dbname, name)) {
            throw new MetaException("Unable to drop table");
          }
          success  = getMS().commitTransaction();
          tblPath = new Path(tbl.getSd().getLocation());
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          } else if(deleteData && (tblPath != null)) {
            wh.deleteDir(tblPath, true);
            // ok even if the data is not deleted
          }
        }
      }

      public Table get_table(String dbname, String name) throws MetaException, NoSuchObjectException {
        this.incrementCounter("get_table");
        logStartFunction("get_table", dbname, name);
        Table t = getMS().getTable(dbname, name);
        if(t == null) {
          throw new NoSuchObjectException(dbname + "." + name + " table not found");
        }
        return t;
      }

      public boolean set_table_parameters(String dbname, String name, 
          Map<String, String> params) throws NoSuchObjectException,
          MetaException {
        this.incrementCounter("set_table_parameters");
        logStartFunction("set_table_parameters", dbname, name);
        // TODO Auto-generated method stub
        return false;
      }

      public Partition append_partition(String dbName, String tableName, List<String> part_vals)
          throws InvalidObjectException, AlreadyExistsException, MetaException {
        this.incrementCounter("append_partition");
        logStartFunction("append_partition", dbName, tableName);
        if(LOG.isDebugEnabled()) {
          for (String part : part_vals) {
            LOG.debug(part);
          }
        }
        Partition part = new Partition();
        boolean success = false;
        try {
          getMS().openTransaction();
          part = new Partition();
          part.setDbName(dbName);
          part.setTableName(tableName);
          part.setValues(part_vals);

          Table tbl = getMS().getTable(part.getDbName(), part.getTableName());
          if(tbl == null) {
            throw new InvalidObjectException("Unable to add partition because table or database do not exist");
          }

          part.setSd(tbl.getSd());
          Path partLocation = new Path(tbl.getSd().getLocation(), Warehouse.makePartName(tbl.getPartitionKeys(), part_vals));
          part.getSd().setLocation(partLocation.toString());

          Partition old_part = this.get_partition(part.getDbName(), part.getTableName(), part.getValues());
          if( old_part != null) {
            throw new AlreadyExistsException("Partition already exists:" + part);
          }
          
          success = getMS().addPartition(part);
          if(success) {
            success = getMS().commitTransaction();
          }
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          } else {
            Path path = new Path(part.getSd().getLocation());
            wh.mkdirs(path);
          }
        }
        return part;
      }
      
      public int add_partitions(List<Partition> parts) throws MetaException, InvalidObjectException, AlreadyExistsException {
        this.incrementCounter("add_partition");
        if(parts.size() == 0) {
          return 0;
        }
        String db = parts.get(0).getDbName();
        String tbl = parts.get(0).getTableName();
        logStartFunction("add_partitions", db, tbl);
        boolean success = false;
        try {
          getMS().openTransaction();
          for (Partition part : parts) {
            this.add_partition(part);
          }
          success = true;
          getMS().commitTransaction();
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          }
        }
        return parts.size();
      }

      public Partition add_partition(Partition part) throws InvalidObjectException,
          AlreadyExistsException, MetaException {
        this.incrementCounter("add_partition");
        logStartFunction("add_partition", part.getDbName(), part.getTableName());
        boolean success = false;
        try {
          getMS().openTransaction();
          Partition old_part = this.get_partition(part.getDbName(), part.getTableName(), part.getValues());
          if( old_part != null) {
            throw new AlreadyExistsException("Partition already exists:" + part);
          }
          Table tbl = getMS().getTable(part.getDbName(), part.getTableName());
          if(tbl == null) {
            throw new InvalidObjectException("Unable to add partition because table or database do not exist");
          }
          // add partition
          success = getMS().addPartition(part);
          if(success) {
            success = getMS().commitTransaction();
          }
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          } else {
            Path path = new Path(part.getSd().getLocation());
            wh.mkdirs(path);
          }
        }
        return part;
      }

      public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws NoSuchObjectException, MetaException,
          TException {
        this.incrementCounter("drop_partition");
        logStartFunction("drop_partition", db_name, tbl_name);
        LOG.info("Partition values:" + part_vals);
        boolean success = false;
        Path partPath = null;
        try {
          getMS().openTransaction();
          Partition part = this.get_partition(db_name, tbl_name, part_vals);
          if(part == null) {
            throw new NoSuchObjectException("Partition doesn't exist. " + part_vals);
          }
          if(part.getSd() == null  || part.getSd().getLocation() == null) {
            throw new MetaException("Partition metadata is corrupted");
          }
          if(!getMS().dropPartition(db_name, tbl_name, part_vals)) {
            throw new MetaException("Unable to drop partition");
          }
          success  = getMS().commitTransaction();
          partPath = new Path(part.getSd().getLocation());
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          } else if(deleteData && (partPath != null)) {
            wh.deleteDir(partPath, true);
            // ok even if the data is not deleted
          }
        }
        return true;
      }

      public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
          throws MetaException {
        this.incrementCounter("get_partition");
        logStartFunction("get_partition", db_name, tbl_name);
        return getMS().getPartition(db_name, tbl_name, part_vals);
      }

      public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
          throws NoSuchObjectException, MetaException {
        this.incrementCounter("get_partitions");
        logStartFunction("get_partitions", db_name, tbl_name);
        return getMS().getPartitions(db_name, tbl_name, max_parts);
      }
      
      public List<String> get_partition_names(String db_name, String tbl_name, short max_parts) throws MetaException {
        this.incrementCounter("get_partition_names");
        logStartFunction("get_partition_names", db_name, tbl_name);
        return getMS().listPartitionNames(db_name, tbl_name, max_parts);
      }

      public boolean alter_partitions(StorageDescriptor sd, List<String> parts) throws InvalidOperationException,
          MetaException {
        this.incrementCounter("alter_partitions");
        logStartFunction("alter_partitions");
        // TODO Auto-generated method stub
        throw new MetaException("Not yet implemented");
      }

      
      public boolean set_partition_parameters(String db_name, String tbl_name, String pname, Map<String, String> params) throws NoSuchObjectException,
          MetaException {
        this.incrementCounter("set_partition_parameters");
        logStartFunction("set_partition_parameters: db=" + db_name + " tbl=" + tbl_name);
        // TODO Auto-generated method stub
        throw new MetaException("Not yet implemented");
      }

      public boolean create_index(Index index_def)
          throws IndexAlreadyExistsException, MetaException {
        this.incrementCounter("create_index");
        logStartFunction("truncate_table: db=" + index_def.getTableName() + " tbl=" + index_def.getTableName() + " name=" + index_def.getIndexName());
        // TODO Auto-generated method stub
        throw new MetaException("Not yet implemented");
      }

      public String getVersion() throws TException {
        this.incrementCounter("getVersion");
        logStartFunction("getVersion");
        return "3.0";
      }

      public void alter_table(String dbname, String name, Table newTable) throws InvalidOperationException,
          MetaException {
        this.incrementCounter("alter_table");
        logStartFunction("truncate_table: db=" + dbname + " tbl=" + name + " newtbl=" + newTable.getTableName());
        try {
          getMS().alterTable(dbname, name, newTable);
        } catch (InvalidObjectException e) {
          LOG.error(StringUtils.stringifyException(e));
          throw new InvalidOperationException("alter is not possible");
        }
      }

      @Override
      public List<String> get_tables(String dbname, String pattern) throws MetaException {
        this.incrementCounter("get_tables");
        logStartFunction("get_tables: db=" + dbname + " pat=" + pattern);
        return getMS().getTables(dbname, pattern);
      }


      public List<FieldSchema> get_fields(String db, String tableName) 
        throws MetaException,UnknownTableException, UnknownDBException {
        this.incrementCounter("get_fields");
        logStartFunction("get_fields: db=" + db + "tbl=" + tableName);
        String [] names = tableName.split("\\.");
        String base_table_name = names[0];

        Table tbl;
        try {
          tbl = this.get_table(db, base_table_name);
        } catch (NoSuchObjectException e) {
          throw new UnknownTableException(e.getMessage());
        }
        try {
          Deserializer s = MetaStoreUtils.getDeserializer(this.hiveConf, tbl);
          return MetaStoreUtils.getFieldsFromDeserializer(tableName, s);
        } catch(SerDeException e) {
          StringUtils.stringifyException(e);
          throw new MetaException(e.getMessage());
        }
      }
  }
    
  /**
   * @param args
   */
  public static void main(String[] args) {
    int port = 9083;

    if(args.length > 0) {
      port = Integer.getInteger(args[0]);
    }
    try {
      TServerTransport serverTransport = new TServerSocket(port);
      Iface handler = new HMSHandler("new db based metaserver");
      FacebookService.Processor processor = new ThriftHiveMetastore.Processor(handler);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = 200;
      TServer server = new TThreadPoolServer(processor, serverTransport,
          new TTransportFactory(), new TTransportFactory(),
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
      HMSHandler.LOG.info("Started the new metaserver on port [" + port + "]...");
      HMSHandler.LOG.info("Options.minWorkerThreads = " + options.minWorkerThreads);
      HMSHandler.LOG.info("Options.maxWorkerThreads = " + options.maxWorkerThreads);
      server.serve();
    } catch (Exception x) {
      x.printStackTrace();
    }
  }
}

