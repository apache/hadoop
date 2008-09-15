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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
import org.apache.hadoop.hive.serde.SerDe;
import org.apache.hadoop.hive.serde.SerDeException;
import org.apache.hadoop.hive.serde.SerDeField;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import com.facebook.thrift.TException;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;

/**
 * TODO:pc remove application logic to a separate interface. rename to MetaStoreServer
 */
public class HiveMetaStore extends ThriftHiveMetastore {
  
    public static class HMSHandler extends FacebookBase implements ThriftHiveMetastore.Iface{
      public static final Log LOG = LogFactory.getLog("hive.metastore");
      private HiveConf hiveConf; // stores datastore (jpox) properties, right now they come from jpox.properties
      private RawStore ms; // a metadata store
      private Warehouse wh; // hdfs warehouse
    
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
        String rawStoreClassName = hiveConf.get("hive.metastore.rawstore.impl");
        wh = new Warehouse(hiveConf);
        LOG.info("Opening raw store ... impl class:" + rawStoreClassName);
        ms = (RawStore) ReflectionUtils.newInstance(getClass(rawStoreClassName, RawStore.class), hiveConf);
        // create default database if it doesn't exist
        try {
          ms.getDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
        } catch (NoSuchObjectException e) {
          ms.createDatabase(new Database(MetaStoreUtils.DEFAULT_DATABASE_NAME, 
                    wh.getDefaultDatabasePath(MetaStoreUtils.DEFAULT_DATABASE_NAME).toString()));
        }
        return true;
      }

      private Class<?> getClass(String rawStoreClassName, Class<RawStore> class1) throws MetaException {
        try {
          return Class.forName(rawStoreClassName, true, classLoader);
        } catch (ClassNotFoundException e) {
          throw new MetaException(rawStoreClassName + " class not found");
        }
      }

      @Override
      public int getStatus() {
        return fb_status.ALIVE;
      }
      
      public void shutdown() {
        LOG.info("Shutting down the object store...");
        ms.shutdown();
        super.shutdown();
      }

      public boolean create_database(String name, String location_uri)
      throws AlreadyExistsException, MetaException {
        this.incrementCounter("create_database");
        boolean success = false;
        try {
          ms.openTransaction();
          Database db = new Database(name, location_uri);
          if(ms.createDatabase(db) && wh.mkdirs(wh.getDefaultDatabasePath(name))) {
            success = ms.commitTransaction();
          }
        } finally {
          if(!success) {
            ms.rollbackTransaction();
          }
        }
        return success;
      }

      public Database get_database(String name) throws NoSuchObjectException, MetaException {
        this.incrementCounter("get_database");
        return ms.getDatabase(name);
      }

      public boolean drop_database(String name) throws MetaException {
        this.incrementCounter("drop_database");
        if(name.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
          throw new MetaException("Can't drop default database");
        }
        boolean success = false;
        try {
          ms.openTransaction();
          success = ms.dropDatabase(name);
          if(ms.dropDatabase(name)) {
            success = ms.commitTransaction();
          }
        } finally {
          if(!success) {
            ms.rollbackTransaction();
          } else {
            wh.deleteDir(wh.getDefaultDatabasePath(name), true);
            // it is not a terrible thing even if the data is not deleted
          }
        }
        return success;
      }

      public List<String> get_databases() throws MetaException {
        this.incrementCounter("get_databases");
        return ms.getDatabases();
      }

      public boolean create_type(Type type) throws AlreadyExistsException, MetaException, InvalidObjectException {
        this.incrementCounter("create_type");
        // check whether type already exists
        if(get_type(type.getName()) != null) {
          throw new AlreadyExistsException("Type " + type.getName() + " already exists");
        }

        //TODO:pc Validation of types should be done by clients or here????
        return ms.createType(type);
      }

      public Type get_type(String name) throws MetaException {
        this.incrementCounter("get_type");
        return ms.getType(name);
      }

      public boolean drop_type(String type) throws MetaException {
        this.incrementCounter("drop_type");
        // TODO:pc validate that there are no types that refer to this 
        return ms.dropType(type);
      }

      public Map<String, Type> get_type_all(String name) throws MetaException {
        this.incrementCounter("get_type_all");
        // TODO Auto-generated method stub
        return null;
      }

      public void create_table(Table tbl) throws AlreadyExistsException, MetaException, InvalidObjectException {
        this.incrementCounter("create_table");
        boolean success = false;
        if(!MetaStoreUtils.validateName(tbl.getTableName())) {
          throw new InvalidObjectException(tbl.getTableName() + " is not a valid object name");
        }
        try {
          ms.openTransaction();
          Path tblPath = null;
          if(tbl.getSd().getLocation() == null || tbl.getSd().getLocation().isEmpty()) {
            tblPath = wh.getDefaultTablePath(tbl.getDatabase(), tbl.getTableName());
            tbl.getSd().setLocation(tblPath.toString());
          } else {
            tblPath = new Path(tbl.getSd().getLocation());
          }
          // get_table checks whether database exists, it should be moved here
          try {
            if(get_table(tbl.getDatabase(), tbl.getTableName()) != null) {
              throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists");
            }
          } catch (NoSuchObjectException e) {
          }
          ms.createTable(tbl);
          if(wh.mkdirs(tblPath)) {
            success = ms.commitTransaction();
          }
      
        } finally {
          if(!success) {
            ms.rollbackTransaction();
          }
        }
      }
      
      public void drop_table(String dbname, String name, boolean deleteData) throws NoSuchObjectException, MetaException {
        this.incrementCounter("drop_table");
        boolean success = false;
        Path tblPath = null;
        try {
          ms.openTransaction();
          // drop any partitions
          Table tbl = get_table(dbname, name);
          if (tbl == null) {
            throw new NoSuchObjectException(name + " doesn't exist");
          }
          if(tbl.getSd() == null  || tbl.getSd().getLocation() == null) {
            throw new MetaException("Table metadata is corrupted");
          }
          if(!ms.dropTable(dbname, name)) {
            throw new MetaException("Unable to drop table");
          }
          success  = ms.commitTransaction();
          tblPath = new Path(tbl.getSd().getLocation());
        } finally {
          if(!success) {
            ms.rollbackTransaction();
          } else if(deleteData && (tblPath != null)) {
            wh.deleteDir(tblPath, true);
            // ok even if the data is not deleted
          }
        }
      }

      public Table get_table(String dbname, String name) throws MetaException, NoSuchObjectException {
        this.incrementCounter("get_table");
        Table t = ms.getTable(dbname, name);
        if(t == null) {
          throw new NoSuchObjectException(dbname + "." + name + " table not found");
        }
        return t;
      }

      public boolean set_table_parameters(String dbname, String name, 
          Map<String, String> params) throws NoSuchObjectException,
          MetaException {
        this.incrementCounter("set_table_parameters");
        // TODO Auto-generated method stub
        return false;
      }

      public Partition append_partition(String dbName, String tableName, List<String> part_vals)
          throws InvalidObjectException, AlreadyExistsException, MetaException {
        this.incrementCounter("append_partition");
        Partition part = new Partition();
        boolean success = false;
        try {
          ms.openTransaction();
          part = new Partition();
          part.setDatabase(dbName);
          part.setTableName(tableName);
          part.setValues(part_vals);

          Table tbl = ms.getTable(part.getDatabase(), part.getTableName());
          if(tbl == null) {
            throw new InvalidObjectException("Unable to add partition because table or database do not exist");
          }
          part.setSd(tbl.getSd());
          Path partLocation = new Path(tbl.getSd().getLocation(), Warehouse.makePartName(tbl.getPartitionKeys(), part_vals));
          part.getSd().setLocation(partLocation.toString());

          success = ms.addPartition(part);
          if(success) {
            success = ms.commitTransaction();
          }
        } finally {
          if(!success) {
            ms.rollbackTransaction();
          } else {
            Path path = new Path(part.getSd().getLocation());
            MetaStoreUtils.makeDir(path, hiveConf);
          }
        }
        return part;
      }

      public Partition add_partition(Partition part) throws InvalidObjectException,
          AlreadyExistsException, MetaException {
        this.incrementCounter("add_partition");
        boolean success = false;
        try {
          ms.openTransaction();
          Table tbl = ms.getTable(part.getDatabase(), part.getTableName());
          if(tbl == null) {
            throw new InvalidObjectException("Unable to add partition because table or database do not exist");
          }
          // add partition
          success = ms.addPartition(part);
          if(success) {
            success = ms.commitTransaction();
          }
        } finally {
          if(!success) {
            ms.rollbackTransaction();
          } else {
            Path path = new Path(part.getSd().getLocation());
            MetaStoreUtils.makeDir(path, hiveConf);
          }
        }
        return part;
      }

      public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws NoSuchObjectException, MetaException,
          TException {
        this.incrementCounter("drop_partition");
        // TODO:pc drop the data as needed
        return ms.dropPartition(db_name, tbl_name, part_vals);
      }

      public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
          throws MetaException {
        this.incrementCounter("get_partition");
        return ms.getPartition(db_name, tbl_name, part_vals);
      }

      public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
          throws NoSuchObjectException, MetaException {
        this.incrementCounter("get_partitions");
        return ms.getPartitions(db_name, tbl_name, max_parts);
      }

      public boolean alter_partitions(StorageDescriptor sd, List<String> parts) throws InvalidOperationException,
          MetaException {
        this.incrementCounter("alter_partitions");
        // TODO Auto-generated method stub
        return false;
      }

      
      public boolean set_partition_parameters(String db_name, String tbl_name, String pname, Map<String, String> params) throws NoSuchObjectException,
          MetaException {
        this.incrementCounter("set_partition_parameters");
        // TODO Auto-generated method stub
        return false;
      }

      public boolean create_index(Index index_def)
          throws IndexAlreadyExistsException, MetaException {
        this.incrementCounter("create_index");
        // TODO Auto-generated method stub
        return false;
      }

      public String getVersion() throws TException {
        this.incrementCounter("getVersion");
        return "3.0";
      }

      public void alter_table(String dbname, String name, Table newTable) throws InvalidOperationException,
          MetaException {
        this.incrementCounter("alter_table");
        try {
          ms.alterTable(dbname, name, newTable);
        } catch (InvalidObjectException e) {
          LOG.error(StringUtils.stringifyException(e));
          throw new InvalidOperationException("alter is not possible");
        }
      }

      @Override
      public List<String> cat(String db_name, String table_name, String partition, int high)
          throws MetaException, UnknownDBException, UnknownTableException {
        this.incrementCounter("cat");
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<String> get_tables(String dbname, String pattern) throws MetaException {
        this.incrementCounter("get_tables");
        return ms.getTables(dbname, pattern);
      }

      @Override
      public void truncate_table(String db_name, String table_name, String partition)
          throws MetaException, UnknownTableException, UnknownDBException {
        // TODO Auto-generated method stub
        this.incrementCounter("truncate_table");
      }
      
      /**
       * normalizeType
       *
       * For pretty printing
       *
       * @param type a type name
       * @return cleaned up - remove Java.lang and make numbers into int , ...
       */
      public String normalizeType(String type) {
        Pattern tpat = Pattern.compile("java.lang.");
        Matcher m = tpat.matcher(type);
        type = m.replaceFirst("");
        String ret = type;
        if(type.equals("String")) {
          ret = "string";
        }  else if(type.equals("Integer")) {
          ret = "int";
        }
        return ret;

      }

      /**
       * hfToStrting
       *
       * Converts a basic SerDe's type to a string. It doesn't do any recursion.
       *
       * @param f the hive field - cannot be null!
       * @return a string representation of the field's type
       * @exception MetaException - if the SerDe raises it or the field is null
       *
       */
      private String hfToString(SerDeField f) throws MetaException {
        String ret;

        try {
          if(f.isPrimitive()) {
            ret = this.normalizeType(f.getType().getName());
          } else if(f.isList()) {
            ret = "List<" +  this.normalizeType(f.getListElementType().getName()) + ">";
          } else if(f.isMap()) {
            ret = "Map<" + this.normalizeType(f.getMapKeyType().getName()) + "," +
              this.normalizeType(f.getMapValueType().getName()) + ">";
          } else {
            // complex type and we just show the type name of the complex type
            ret = f.getName();
          }
        }  catch(Exception e) {
          StringUtils.stringifyException(e);
          throw new MetaException(e.getMessage());
        }
        return ret;
      }

      public ArrayList<FieldSchema> get_fields(String db, String table_name) throws MetaException,UnknownTableException, UnknownDBException {
        this.incrementCounter("get_fields");
        ArrayList<FieldSchema> str_fields = new ArrayList<FieldSchema>();
        String [] names = table_name.split("\\.");
        String base_table_name = names[0];
        List<SerDeField> hive_fields = new ArrayList<SerDeField>();

        Table tbl;
        try {
          tbl = this.get_table(db, base_table_name);
        } catch (NoSuchObjectException e) {
          throw new UnknownTableException(e.getMessage());
        }
        SerDeField hf = null;
        try {
          // TODO:pc getSerDe requires 'columns' field in some cases and this doesn't supply
          SerDe s = MetaStoreUtils.getSerDe(this.hiveConf, tbl);

          // recurse down the type.subtype.subsubtype expression until at the desired type
          for(int i =  1; i < names.length; i++) {
            hf = s.getFieldFromExpression(hf,names[i]);
          }

          // rules on how to recurse the SerDe based on its type
          if(hf != null && hf.isPrimitive()) {
            hive_fields.add(hf);
          } else if(hf != null && hf.isList()) {
            // don't remember why added this rule??
            // should just be a hive_fields.add(hf)
            try {
              hive_fields = s.getFields(hf);
            } catch(Exception e) {
              hive_fields.add(hf);
            }
          } else if(hf != null && hf.isMap()) {
            hive_fields.add(hf);
          } else {
            hive_fields = s.getFields(hf);
          }

          for(SerDeField field: hive_fields) {
            String name = field.getName();
            String schema = this.hfToString(field);
            str_fields.add(new FieldSchema(name, schema, "automatically generated"));
          }
          return str_fields;
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
      TServer server = new TThreadPoolServer(processor, serverTransport);
      HMSHandler.LOG.info("Starting the new metaserver on port [" + port + "]...");

      server.serve();

    } catch (Exception x) {
      x.printStackTrace();
    }
  }

}

