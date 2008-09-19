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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftMetaStore;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.util.StringUtils;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import com.facebook.thrift.TApplicationException;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;
import com.facebook.thrift.transport.TTransportFactory;


/**
 * MetaStore thrift Service Implementation
 *
 * A thrift wrapper around the MetaStore exposing most of its interfaces to any thrift client.
 *
 *
 *
 */
public class MetaStoreServer extends ThriftMetaStore {

  public static class ThriftMetaStoreHandler extends FacebookBase implements ThriftMetaStore.Iface
  {

    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.hive.metastore");


    public void setOption(String key, String val) {
    }


    /**
     * getStatus
     *
     * @return ALIVE
     */
    public int getStatus() {
      return fb_status.ALIVE;
    }

    /**
     * getVersion
     *
     * @return current version of the store. Should proxy this request to the MetaStore class actually!
     */
    public String getVersion() {
      return "0.1";
    }

    /**
     * shutdown
     *
     * cleanly closes everything and exit.
     */
    public void shutdown() {
      LOG.info("MetaStoreServer shutting down.");
      throw new RuntimeException("Shutting down because of an explicit shutdown command.");
    }

    Configuration conf_;

    /**
     * ThriftMetaStoreHandler
     *
     * Constructor for the MetaStore glue with Thrift Class.
     *
     * @param name - the name of this handler
     */
    public ThriftMetaStoreHandler(String name) {
      super(name);
      conf_ = new HiveConf(ThriftMetaStoreHandler.class);
    }
    public ThriftMetaStoreHandler(String name, Configuration configuration) {
      super(name);
      conf_ = configuration;
    }

    // NOTE - pattern does not apply to dbs - the prefix must be a valid db or assumes default db
    public ArrayList<String> get_tables(String dbName, String pattern) throws TException, MetaException, UnknownDBException   {
      this.incrementCounter("get_tables");
      LOG.info("get_tables(" + dbName + "," + pattern + ")");
      try {
        DB db = new DB(dbName, conf_);
        return db.getTables(pattern);
      } catch(MetaException e) {
        this.incrementCounter("exceptions");
        LOG.error("Got exception in get_tables: " + e.getMessage());
        e.printStackTrace();
        throw new TApplicationException("MetaException getting tables:" + e.getMessage());
      } catch(RuntimeException e) {
        LOG.fatal("get_tables got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("get_tables had an internal Java RuntimeException: " + e.getMessage());
      }
    }



    public ArrayList<String> cat(String dbName, String tableName, String partition,int num) throws TException,
                                                                                                   MetaException,
                                                                                                   UnknownDBException,
                                                                                                   UnknownTableException {
      this.incrementCounter("cat");
      LOG.info("cat(" + tableName + "," + partition + "," + num + ")");
      //            return Cat.cat(tableName, partition, num);
      return null;
    }

    /**
     * get_dbs
     *
     * @return a list of all the dbs
     * @exception TException if Thrift problem.
     * @exception MetaException if internal meta store problem
     */
    public List<String> get_dbs() throws TException, MetaException {
      this.incrementCounter("get_dbs");
      LOG.info("get_dbs()");
      try {
        return new FileStore(conf_).getDatabases();
      } catch(MetaException e) {
        this.incrementCounter("exceptions");
        LOG.error("Got exception in get_dbs: " + e.getMessage());
        e.printStackTrace();
        throw new TApplicationException("MetaException getting dbs:" + e.getMessage());
      } catch(RuntimeException e) {
        LOG.fatal("get_dbs got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("get_dbs had an internal Java RuntimeException: " + e.getMessage());
      }
    }


    /**
     * getPartitions
     *
     * return a table's partitions
     *
     * @param tableName - a valid table name
     * @param low - low index
     * @param high - high index
     * @return a string (including '\n's) of the rows
     * @exception TException if thrift problem
     * @exception MetaException if internal problem or bad input
     * @exception UnknownTableException if we don't know about this table.
     */
    public ArrayList<String> get_partitions(String dbName, String tableName) throws TException,MetaException,UnknownTableException, UnknownDBException  {
      this.incrementCounter("getPartitions");
      LOG.info("getPartitions(" + dbName + "," + tableName + ")");
      return new DB(dbName, conf_).getTable(tableName, true).getPartitions();
    }

    /**
     * table_exists
     *
     * check if the table's schema exists in the metastore
     *
     * @param tableName - a valid table name
     * @return true/false
     * @exception TException if thrift problem
     * @exception MetaException if internal problem or bad input
     */
    public boolean table_exists(String dbName, String tableName)  throws TException,MetaException, UnknownDBException {
      this.incrementCounter("schema_exists");
      LOG.info("schema_exists(" + dbName + "," + tableName + ")");
      try {
        return new DB(dbName, conf_).tableExists(tableName);
      } catch(MetaException e) {
        LOG.error("Got exception in schema_exists: " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(UnknownDBException e) {
        LOG.error("Got exception in schema_exists: " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(RuntimeException e) {
        LOG.fatal("schema_exists got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("schema_exists had an internal Java RuntimeException: " + e.getMessage());
      }
    }

    /**
     * alter_table
     *
     * For a columnsetSerDe table, add column names to it
     *
     * @param tableName - a valid existing table name
     * @param columns - ordered list of column names
     * @exception TException if thrift problem
     * @exception MetaException if internal problem or bad input
     * @exception UnknownTableException if table does not exist already
     */
    public void alter_table(String dbName, String  tableName,  Map<String, String> schema)  throws TException,MetaException,UnknownTableException, UnknownDBException {
      this.incrementCounter("alter_table");
      LOG.info("alter_table(" + dbName + "," + tableName + "," + schema + ")");
      try {
        Properties p = new Properties();
        for(Iterator<Entry<String, String>> it = schema.entrySet().iterator(); it.hasNext() ; ) {
          Entry<String, String> entry = it.next();
          p.setProperty(entry.getKey(), entry.getValue());
        }
        new DB(dbName, conf_).getTable(tableName, false).alter(p);
      } catch(MetaException e) {
        LOG.error("Got exception in alter_table: " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(RuntimeException e) {
        LOG.fatal("alter_table got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("alter_table had an internal Java RuntimeException: " + e.getMessage());
      }
    }

    /**
     * create_table
     *
     * Create names columns for a columnset type table
     *
     * @param tableName - a valid table name
     * @param columns - ordered list of column names
     * @exception TException if thrift problem
     * @exception MetaException if internal problem or bad input
     */
    public void create_table(String dbName, String tableName, Map<String, String> schema)  throws TException,MetaException, UnknownDBException {
      this.incrementCounter("create_table");
      LOG.info("create_table(" + dbName + "," + tableName + ")");
      try {
        Properties p = new Properties();
        for(Iterator<Entry<String, String>> it = schema.entrySet().iterator(); it.hasNext() ; ) {
          Entry<String, String> entry = it.next();
          p.setProperty(entry.getKey(), entry.getValue());
        }
        DB db = new DB(dbName, conf_);
        RWTable.create(db,tableName,p, conf_);
      } catch(MetaException e) {
        LOG.error("Got exception in create_table: " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(UnknownDBException e) {
        LOG.error("Got exception in create_table: " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(RuntimeException e) {
        LOG.fatal("create_table got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("create_table had an internal Java RuntimeException: " + e.getMessage());
      }
    }


    public ArrayList<FieldSchema> get_fields(String db, String table_name) throws MetaException,UnknownTableException, UnknownDBException {

      ArrayList<FieldSchema> str_fields = new ArrayList<FieldSchema>();
      String [] names = table_name.split("\\.");
      String base_table_name = names[0];
      String last_name = names[names.length-1];

      AbstractMap<String,String> schema_map = get_schema(base_table_name); // will throw UnknownTableException if not found
      Properties p = new Properties();
      for(Iterator<Entry<String, String>> it = schema_map.entrySet().iterator(); it.hasNext() ; ) {
        Entry<String, String> entry = it.next();
        p.setProperty(entry.getKey(), entry.getValue());
      }
      // xxx

      try {
        //            Table t = Table.readTable(p, this.db);
        Deserializer s = MetaStoreUtils.getDeserializer( conf_, p);
        ObjectInspector oi = s.getObjectInspector();
        
        // recurse down the type.subtype.subsubtype expression until at the desired type
        for(int i =  1; i < names.length; i++) {
          if (!(oi instanceof StructObjectInspector)) {
            oi = s.getObjectInspector();
            break;
          }
          StructObjectInspector soi = (StructObjectInspector)oi;
          StructField sf = soi.getStructFieldRef(names[i]);
          if (sf == null) {
            // If invalid field, then return the schema of the table
            oi = s.getObjectInspector();
            break;
          } else {
            oi = sf.getFieldObjectInspector();
          }
        }

        // rules on how to recurse the SerDe based on its type
        if (oi.getCategory() != Category.STRUCT) {
          str_fields.add(new FieldSchema(last_name, oi.getTypeName(), "automatically generated"));
        } else {
          List<? extends StructField> fields = ((StructObjectInspector)oi).getAllStructFieldRefs();
          for(int i=0; i<fields.size(); i++) {
            String fieldName = fields.get(i).getFieldName();
            String fieldTypeName = fields.get(i).getFieldObjectInspector().getTypeName();
            str_fields.add(new FieldSchema(fieldName, fieldTypeName, "automatically generated"));
          }
        }
        return str_fields;

      } catch(SerDeException e) {
        StringUtils.stringifyException(e);
        MetaException m = new MetaException();
        m.setMessage(e.getMessage());
        throw m;
      }

    }


    /**
     * drop
     *
     * drop a table
     *
     * @param tableName - a valid existing table name
     * @param delete_data - should the store auto delete the data.
     * @exception TException if thrift problem
     * @exception MetaException if internal problem or bad input
     * @exception UnknownTableException if table does not exist already
     */
    public void drop_table(String dbName, String tableName)  throws TException,MetaException,UnknownTableException, UnknownDBException {
      this.incrementCounter("drop");
      LOG.info("drop(" + dbName + "," + tableName + ")");
      try {
        new DB(dbName, conf_).getTable(tableName, false).drop();
      } catch(MetaException e) {
        LOG.error("Got exception in drop: " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(RuntimeException e) {
        LOG.fatal("drop got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("drop had an internal Java RuntimeException: " + e.getMessage());
      }
    }

    /**
     * truncate
     *
     * drop a table
     *
     * @param tableName - a valid existing table name
     * @param delete_data - should the store auto delete the data.
     * @exception TException if thrift problem
     * @exception MetaException if internal problem or bad input
     * @exception UnknownTableException if table does not exist already
     */
    public void truncate_table(String dbName, String tableName, String partition)  throws TException,MetaException,UnknownTableException, UnknownDBException {
      this.incrementCounter("truncate");
      LOG.info("truncate_table(" + dbName + "," + tableName + "," + partition + ")");
      try {
        new DB(dbName, conf_).getTable(tableName, false).truncate(partition);
      } catch(MetaException e) {
        LOG.error("Got exception in truncate " + e.getMessage());
        this.incrementCounter("exceptions");
        throw e;
      } catch(RuntimeException e) {
        LOG.fatal("truncate got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("drop had an internal Java RuntimeException: " + e.getMessage());
      }
    }

    /**
     * get_schema
     *
     * Gets the (opaque) schema which is currently represented as a key=>value map.
     *
     * @param name - the name of the table
     * @return the key/value of the opaque schema
     * @exception MetaException if internal problem
     * @exception UnknownTableException if the table doesn't exist
     */
    public AbstractMap<String, String> get_schema(String tableName) throws MetaException, UnknownTableException, UnknownDBException {
      this.incrementCounter("get_schema");
      String dbName = "default";
      LOG.debug("get_schema(" + dbName + "," + tableName + ")");
      try {
        Table t = new DB(dbName, conf_).getTable(tableName, true);
        Properties p = t.getSchema();
        HashMap<String, String> ret = new HashMap<String, String> ();
        for(Enumeration<?> e = p.propertyNames(); e.hasMoreElements() ; ) {
          String key = (String)e.nextElement();
          String val = p.getProperty(key);
          ret.put(key,val);
        }
        return ret;
      } catch(MetaException e) {
        // later get better at this so can differentiate internal error and missing table
        this.incrementCounter("get_schema_exceptions");
        e.printStackTrace();
        LOG.error("Got exception in get_schema: " + e.getMessage());
        throw e;
      } catch(RuntimeException e) {
        LOG.fatal("get_schema got a runtime exception: " + e.getMessage());
        MetaStoreUtils.printStackTrace(e);
        throw new MetaException("get_schema had an internal Java RuntimeException: " + e.getMessage());
      }

    }
  }
  public static void main(String [] args) {
    int port = 9082;


    if(args.length > 0) {
      port = Integer.getInteger(args[0]);
    }
    try {
      TServerTransport serverTransport = new TServerSocket(port);
      Iface handler = new ThriftMetaStoreHandler("hello");
      FacebookService.Processor processor = new ThriftMetaStore.Processor(handler);

      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = 100;
      TServer server = new TThreadPoolServer(processor, serverTransport,
               new TTransportFactory(), new TTransportFactory(),
               new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);

      ThriftMetaStoreHandler.LOG.info("Starting the metaserver on port [" + port + "]...");

      server.serve();

    } catch (Exception x) {
      x.printStackTrace();
    }
  }


};

