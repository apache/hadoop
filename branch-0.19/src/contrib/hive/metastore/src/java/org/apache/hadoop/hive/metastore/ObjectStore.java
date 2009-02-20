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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is the interface between the application logic and the database store that
 * contains the objects. 
 * Refrain putting any logic in mode.M* objects or in this file as former could be auto
 * generated and this class would need to be made into a interface that can read both
 * from a database and a filestore.
 */
public class ObjectStore implements RawStore, Configurable {
  @SuppressWarnings("nls")
  private static final String JPOX_CONFIG = "jpox.properties";
  private static Properties prop = null;
  private static PersistenceManagerFactory pmf = null;
  private static final Log LOG = LogFactory.getLog(ObjectStore.class.getName());
  private static enum TXN_STATUS {
    NO_STATE,
    OPEN,
    COMMITED,
    ROLLBACK
  }
  private boolean isInitialized = false;
  private PersistenceManager pm = null;
  private Configuration hiveConf;
  private int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
  
  public ObjectStore() {}

  @Override
  public Configuration getConf() {
    return hiveConf;
  }

  @Override
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    this.hiveConf = conf;
    if(isInitialized) {
      return;
    } else {
      initialize();
    }
    if(!isInitialized) {
      throw new RuntimeException("Unable to create persistence manager. Check dss.log for details");
    } else {
      LOG.info("Initialized ObjectStore");
    }
  }

  private ClassLoader classLoader;
  {
    this.classLoader = Thread.currentThread().getContextClassLoader();
    if (this.classLoader == null) {
      this.classLoader = ObjectStore.class.getClassLoader();
    }
  }

  @SuppressWarnings("nls")
  private void initialize() {
    LOG.info("ObjectStore, initialize called");
    initDataSourceProps();
    pm = getPersistenceManager();
    if(pm != null)
      isInitialized = true;
    return;
  }

  /**
   * Properties specified in hive-default.xml override the properties specified in
   * jpox.properties.
   */
  @SuppressWarnings("nls")
  private void initDataSourceProps() {
    if(prop != null) {
      return;
    }
    URL url= classLoader.getResource(JPOX_CONFIG);
    prop = new Properties();
    if (url == null) {
      LOG.info(JPOX_CONFIG + " not found.");
    } else {
      LOG.info("found resource " + JPOX_CONFIG + " at " + url);
      try {
        InputStream is = url.openStream();
        if (is == null) {
          throw new RuntimeException("Properties file " + url + " couldn't be opened");
        }
        prop.load(is);
      } catch (IOException ex) {
        throw new RuntimeException("could not load: " + JPOX_CONFIG, ex);
      }
    }
    
    Iterator<Map.Entry<String, String>> iter = hiveConf.iterator();
    while(iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      if(e.getKey().contains("jpox") || e.getKey().contains("jdo")) {
        Object prevVal = prop.setProperty(e.getKey(), e.getValue());
        if(LOG.isDebugEnabled()) {
          LOG.debug("Overriding " + e.getKey() + " value " + prevVal 
              + " from  jpox.properties with " + e.getValue());
        }
      }
    }

    if(LOG.isDebugEnabled()) {
      for (Entry<Object, Object> e: prop.entrySet()) {
        LOG.debug(e.getKey() + " = " + e.getValue());
      }
    }
  }

  private static PersistenceManagerFactory getPMF() {
    if(pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if(dsc != null) {
        dsc.pinAll(true, MTable.class);
        dsc.pinAll(true, MStorageDescriptor.class);
        dsc.pinAll(true, MSerDeInfo.class);
        dsc.pinAll(true, MPartition.class);
        dsc.pinAll(true, MDatabase.class);
        dsc.pinAll(true, MType.class);
        dsc.pinAll(true, MFieldSchema.class);
        dsc.pinAll(true, MOrder.class);
      }
    }
    return pmf;
  }
  
  private PersistenceManager getPersistenceManager() {
    return getPMF().getPersistenceManager();
  }
  
  public void shutdown() {
    if(pm != null) {
      pm.close();
    }
  }

  /**
   * Opens a new one or the one already created
   * Every call of this function must have corresponding commit or rollback function call
   * @return an active transaction
   */
  
  public boolean openTransaction() {
    this.openTrasactionCalls++;
    if(this.openTrasactionCalls == 1) {
      currentTransaction = pm.currentTransaction();
      currentTransaction.begin();
      transactionStatus = TXN_STATUS.OPEN;
    } else {
      // something is wrong since openTransactionCalls is greater than 1 but currentTransaction is not active
      assert((currentTransaction != null) && (currentTransaction.isActive()));
    }
    return currentTransaction.isActive();
  }
  
  /**
   * if this is the commit of the first open call then an actual commit is called. 
   * @return 
   */
  @SuppressWarnings("nls")
  public boolean commitTransaction() {
    assert(this.openTrasactionCalls >= 1);
    if(!currentTransaction.isActive()) {
      throw new RuntimeException("Commit is called, but transaction is not active. Either there are" +
          "mismatching open and close calls or rollback was called in the same trasaction");
    }
    this.openTrasactionCalls--;
    if ((this.openTrasactionCalls == 0) && currentTransaction.isActive()) {
      transactionStatus = TXN_STATUS.COMMITED;
      currentTransaction.commit();
    }
    return true;
  }
  
  /**
   * @return true if there is an active transaction. If the current transaction is either
   * committed or rolled back it returns false
   */
  public boolean isActiveTransaction() {
    if(currentTransaction == null)
      return false;
    return currentTransaction.isActive();
  }
  
  /**
   * Rolls back the current transaction if it is active
   */
  public void rollbackTransaction() {
    if(this.openTrasactionCalls < 1) {
      return;
    }
    this.openTrasactionCalls = 0;
    if(currentTransaction.isActive() && transactionStatus != TXN_STATUS.ROLLBACK) {
      transactionStatus = TXN_STATUS.ROLLBACK;
       // could already be rolled back
      currentTransaction.rollback();
    }
  }

  public boolean createDatabase(Database db) {
    boolean success = false;
    boolean commited = false;
    MDatabase mdb = new MDatabase(db.getName().toLowerCase(), db.getDescription());
    try {
      openTransaction();
      pm.makePersistent(mdb);
      success = true;
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }
  
  public boolean createDatabase(String name) {
    // TODO: get default path
    Database db = new Database(name, "default_path");
    return this.createDatabase(db);
  }
  
  @SuppressWarnings("nls")
  private MDatabase getMDatabase(String name) throws NoSuchObjectException {
    MDatabase db = null;
    boolean commited = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      Query query = pm.newQuery(MDatabase.class, "name == dbname");
      query.declareParameters("java.lang.String dbname");
      query.setUnique(true);
      db = (MDatabase) query.execute(name.trim());
      pm.retrieve(db);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
      if(db == null) {
        throw new NoSuchObjectException("There is no database named " + name);
      }
    }
    return db;
  }
  public Database getDatabase(String name) throws NoSuchObjectException {
    MDatabase db = null;
    boolean commited = false;
    try {
      openTransaction();
      db = getMDatabase(name);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return new Database(db.getName(), db.getDescription());
  }

  public boolean dropDatabase(String dbname) {
    
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      
      // first drop tables
      dbname = dbname.toLowerCase();
      LOG.info("Dropping database along with all tables " + dbname);
      Query q1 = pm.newQuery(MTable.class, "database.name == dbName");
      q1.declareParameters("java.lang.String dbName");
      List<MTable> mtbls = (List<MTable>) q1.execute(dbname.trim());
      pm.deletePersistentAll(mtbls);

      // then drop the database
      Query query = pm.newQuery(MDatabase.class, "name == dbName"); 
      query.declareParameters("java.lang.String dbName"); 
      query.setUnique(true); 
      MDatabase db = (MDatabase) query.execute(dbname.trim()); 
      pm.retrieve(db);
      
      //StringIdentity id = new StringIdentity(MDatabase.class, dbname);
      //MDatabase db = (MDatabase) pm.getObjectById(id);
      if(db != null)
        pm.deletePersistent(db);
      commited = commitTransaction();
      success = true;
    } catch (JDOObjectNotFoundException e) {
      LOG.debug("database not found " + dbname,e);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
        success = false;
      }
    }
    return success;
  }

  public List<String> getDatabases() {
    List dbs = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MDatabase.class);
      query.setResult("name");
      query.setResultClass(String.class);
      query.setOrdering("name asc");
      dbs = (List) query.execute();
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return dbs;
  }
  
  private MType getMType(Type type) {
    List<MFieldSchema> fields = new ArrayList<MFieldSchema>();
    if(type.getFields() != null) {
      for (FieldSchema field : type.getFields()) {
        fields.add(new MFieldSchema(field.getName(), field.getType(), field.getComment()));
      }
    }
    return new MType(type.getName(), type.getType1(), type.getType2(), fields);
  }

  private Type getType(MType mtype) {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    if(mtype.getFields() != null) {
      for (MFieldSchema field : mtype.getFields()) {
        fields.add(new FieldSchema(field.getName(), field.getType(), field.getComment()));
      }
    }
    return new Type(mtype.getName(), mtype.getType1(), mtype.getType2(), fields);
  }

  public boolean createType(Type type) {
    boolean success = false;
    MType mtype = getMType(type);
    boolean commited = false;
    try {
      openTransaction();
      pm.makePersistent(mtype);
      commited = commitTransaction();
      success = true;
    } finally {
      if(!commited) {
        rollbackTransaction();
        success = false;
      }
    }
    return success;
  }

  public Type getType(String typeName) {
    Type type = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName"); 
      query.declareParameters("java.lang.String typeName"); 
      query.setUnique(true); 
      MType mtype = (MType) query.execute(typeName.trim()); 
      pm.retrieve(type);
      if(mtype != null) {
        type = getType(mtype);
      }
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return type;
  }

  public boolean dropType(String typeName) {
    
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName"); 
      query.declareParameters("java.lang.String typeName"); 
      query.setUnique(true); 
      MType type = (MType) query.execute(typeName.trim()); 
      pm.retrieve(type);
      pm.deletePersistent(type);
      commited = commitTransaction();
      success = true;
    } catch (JDOObjectNotFoundException e) {
      commited = commitTransaction();
      LOG.debug("type not found " + typeName, e);
    } finally {
      if(!commited) {
        rollbackTransaction();
        success = false;
      }
    }
    return success;
  }

  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MTable mtbl = convertToMTable(tbl);
      pm.makePersistent(mtbl);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
  }
  
  public boolean dropTable(String dbName, String tableName) {
    
    boolean success = false;
    try {
      openTransaction();
      MTable tbl = getMTable(dbName, tableName); 
      pm.retrieve(tbl);
      if(tbl != null) {
        // first remove all the partitions
        pm.deletePersistentAll(listMPartitions(dbName, tableName, -1));
        // then remove the table
        pm.deletePersistent(tbl);
      }
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public Table getTable(String dbName, String tableName) throws MetaException {
    boolean commited = false;
    Table tbl = null;
    try {
      openTransaction();
      tbl = convertToTable(getMTable(dbName, tableName));
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return tbl;
  }
  
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    boolean commited = false;
    List<String> tbls = null;
    try {
      openTransaction();
      dbName = dbName.toLowerCase();
      // Take the pattern and split it on the | to get all the composing patterns
      String [] subpatterns = pattern.trim().split("\\|");
      String query = "select tableName from org.apache.hadoop.hive.metastore.model.MTable where database.name == dbName && (";
      boolean first = true;
      for(String subpattern: subpatterns) {
        subpattern = "(?i)" + subpattern.replaceAll("\\*", ".*");
        if (!first) {
          query = query + " || ";
        }
        query = query + " tableName.matches(\"" + subpattern + "\")";
        first = false;
      }
      query = query + ")";

      Query q = pm.newQuery(query);
      q.declareParameters("java.lang.String dbName");
      q.setResult("tableName");
      Collection names = (Collection) q.execute(dbName.trim());
      tbls = new ArrayList<String>(); 
      for (Iterator i = names.iterator (); i.hasNext ();) {
          tbls.add((String) i.next ()); 
      }
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return tbls;
  }
  
  private MTable getMTable(String db, String table) {
    MTable mtbl = null;
    boolean commited = false;
    try {
      openTransaction();
      db = db.toLowerCase();
      table = table.toLowerCase();
      Query query = pm.newQuery(MTable.class, "tableName == table && database.name == db"); 
      query.declareParameters("java.lang.String table, java.lang.String db"); 
      query.setUnique(true); 
      mtbl = (MTable) query.execute(table.trim(), db.trim()); 
      pm.retrieve(mtbl);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return mtbl;
  }

  private Table convertToTable(MTable mtbl) throws MetaException {
    if(mtbl == null) return null;
    return new Table(mtbl.getTableName(),
        mtbl.getDatabase().getName(),
        mtbl.getOwner(),
        mtbl.getCreateTime(),
        mtbl.getLastAccessTime(),
        mtbl.getRetention(),
        convertToStorageDescriptor(mtbl.getSd()),
        convertToFieldSchemas(mtbl.getPartitionKeys()),
        mtbl.getParameters());
  }
  
  private MTable convertToMTable(Table tbl) throws InvalidObjectException, MetaException {
    if(tbl == null) return null;
    MDatabase mdb = null;
    try {
      mdb = this.getMDatabase(tbl.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new InvalidObjectException("Database " + tbl.getDbName() + " doesn't exsit.");
    }
    return new MTable(tbl.getTableName().toLowerCase(),
        mdb,
        convertToMStorageDescriptor(tbl.getSd()),
        tbl.getOwner(),
        tbl.getCreateTime(),
        tbl.getLastAccessTime(),
        tbl.getRetention(),
        convertToMFieldSchemas(tbl.getPartitionKeys()),
        tbl.getParameters());
  }
  
  private List<MFieldSchema> convertToMFieldSchemas(List<FieldSchema> keys) {
    List<MFieldSchema> mkeys = null;
    if(keys != null) {
      mkeys = new ArrayList<MFieldSchema>(keys.size());
      for (FieldSchema part : keys) {
        mkeys.add(new MFieldSchema(part.getName().toLowerCase(), part.getType(), part.getComment()));
      }
    }
    return mkeys;
  } 
  
  private List<FieldSchema> convertToFieldSchemas(List<MFieldSchema> mkeys) {
    List<FieldSchema> keys = null;
    if(mkeys != null) {
      keys = new ArrayList<FieldSchema>(mkeys.size());
      for (MFieldSchema part : mkeys) {
        keys.add(new FieldSchema(part.getName(), part.getType(), part.getComment()));
      }
    }
    return keys;
  }
  
  private List<MOrder> convertToMOrders(List<Order> keys) {
    List<MOrder> mkeys = null;
    if(keys != null) {
      mkeys = new ArrayList<MOrder>(keys.size());
      for (Order part : keys) {
        mkeys.add(new MOrder(part.getCol().toLowerCase(), part.getOrder()));
      }
    }
    return mkeys;
  } 
  
  private List<Order> convertToOrders(List<MOrder> mkeys) {
    List<Order> keys = null;
    if(mkeys != null) {
      keys = new ArrayList<Order>();
      for (MOrder part : mkeys) {
        keys.add(new Order(part.getCol(), part.getOrder()));
      }
    }
    return keys;
  }
  
  private SerDeInfo converToSerDeInfo(MSerDeInfo ms) throws MetaException {
   if(ms == null) throw new MetaException("Invalid SerDeInfo object");
   return new SerDeInfo(ms.getName(),
       ms.getSerializationLib(),
       ms.getParameters()); 
  }
  
  private MSerDeInfo converToMSerDeInfo(SerDeInfo ms) throws MetaException {
    if(ms == null) throw new MetaException("Invalid SerDeInfo object");
    return new MSerDeInfo(ms.getName(),
        ms.getSerializationLib(),
        ms.getParameters()); 
   }
  
  // MSD and SD should be same objects. Not sure how to make then same right now
  // MSerdeInfo *& SerdeInfo should be same as well
  private StorageDescriptor convertToStorageDescriptor(MStorageDescriptor msd) throws MetaException {
    if(msd == null) return null;
    return new StorageDescriptor(
        convertToFieldSchemas(msd.getCols()),
        msd.getLocation(),
        msd.getInputFormat(),
        msd.getOutputFormat(),
        msd.isCompressed(),
        msd.getNumBuckets(),
        converToSerDeInfo(msd.getSerDeInfo()),
        msd.getBucketCols(),
        convertToOrders(msd.getSortCols()),
        msd.getParameters());
  }
  
  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd) throws MetaException {
    if(sd == null) return null;
    return new MStorageDescriptor(
        convertToMFieldSchemas(sd.getCols()),
        sd.getLocation(),
        sd.getInputFormat(),
        sd.getOutputFormat(),
        sd.isCompressed(),
        sd.getNumBuckets(),
        converToMSerDeInfo(sd.getSerdeInfo()),
        sd.getBucketCols(),
        convertToMOrders(sd.getSortCols()),
        sd.getParameters());
  }
  
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      MPartition mpart = convertToMPart(part);
      pm.makePersistent(mpart);
      commited = commitTransaction();
      success = true;
    } finally {
      if(!commited) {
        rollbackTransaction();
        success = false;
      }
    }
    return success;
  }
  
  public Partition getPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
    this.openTransaction();
    Partition part = convertToPart(this.getMPartition(dbName, tableName, part_vals));
    this.commitTransaction();
    return part;
  }
  
  private MPartition getMPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
    MPartition mpart = null;
    boolean commited = false;
    try {
      openTransaction();
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();
      MTable mtbl = this.getMTable(dbName, tableName);
      if(mtbl == null) {
        commited = commitTransaction();
        return null;
      }
      // Change the query to use part_vals instead of the name which is redundant
      String name = Warehouse.makePartName(convertToFieldSchemas(mtbl.getPartitionKeys()), part_vals);
      Query query = pm.newQuery(MPartition.class, "table.tableName == t1 && table.database.name == t2 && partitionName == t3"); 
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3"); 
      query.setUnique(true); 
      mpart = (MPartition) query.execute(tableName.trim(), dbName.trim(), name); 
      pm.retrieve(mpart);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return mpart;
  }
  
  private MPartition convertToMPart(Partition part) throws InvalidObjectException, MetaException {
    if(part == null) {
      return null;
    }
    MTable mt = getMTable(part.getDbName(), part.getTableName());
    if(mt == null) {
      throw new InvalidObjectException("Partition doesn't have a valid table or database name");
    }
    return new MPartition(
        Warehouse.makePartName(convertToFieldSchemas(mt.getPartitionKeys()), part.getValues()),
        mt,
        part.getValues(),
        part.getCreateTime(),
        part.getLastAccessTime(),
        convertToMStorageDescriptor(part.getSd()),
        part.getParameters());
  }
  
  private Partition convertToPart(MPartition mpart) throws MetaException {
    if(mpart == null) {
      return null;
    }
    return new Partition(
        mpart.getValues(),
        mpart.getTable().getDatabase().getName(),
        mpart.getTable().getTableName(),
        mpart.getCreateTime(),
        mpart.getLastAccessTime(),
        convertToStorageDescriptor(mpart.getSd()),
        mpart.getParameters());
  }

  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MPartition part = this.getMPartition(dbName, tableName, part_vals); 
      if(part != null)
        pm.deletePersistent(part);
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
        success = false;
      }
    }
    return success;
  }
  
  public List<Partition> getPartitions(String dbName, String tableName, int max) throws MetaException {
    this.openTransaction();
    List<Partition> parts = convertToParts(this.listMPartitions(dbName, tableName, max));
    this.commitTransaction();
    return parts;
  }
  
  private List<Partition> convertToParts(List<MPartition> mparts) throws MetaException {
    List<Partition> parts = new ArrayList<Partition>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(this.convertToPart(mp));
    }
    return parts;
  }


  //TODO:pc implement max
  public List<String> listPartitionNames(String dbName, String tableName, short max) throws MetaException {
    List<String> pns = new ArrayList<String>();
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNames");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();
      Query q = pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition where table.database.name == t1 && table.tableName == t2 order by partitionName asc");
      q.declareParameters("java.lang.String t1, java.lang.String t2");
      q.setResult("partitionName");
      Collection names = (Collection) q.execute(dbName.trim(), tableName.trim());
      pns = new ArrayList<String>(); 
      for (Iterator i = names.iterator (); i.hasNext ();) {
          pns.add((String) i.next ()); 
      }
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
        success = false;
      }
    }
    return pns;
  }
  
  // TODO:pc implement max
  private List<MPartition> listMPartitions(String dbName, String tableName, int max) {
    boolean success = false;
    List<MPartition> mparts = null;
    try {
      openTransaction();
      LOG.debug("Executing listMPartitions");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();
      Query query = pm.newQuery(MPartition.class, "table.tableName == t1 && table.database.name == t2"); 
      query.declareParameters("java.lang.String t1, java.lang.String t2"); 
      mparts = (List<MPartition>) query.execute(tableName.trim(), dbName.trim()); 
      LOG.debug("Done executing query for listMPartitions");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitions");
    } finally {
      if(!success) {
        rollbackTransaction();
        success = false;
      }
    }
    return mparts;
  }

  // TODO: add tests
  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();
      MTable newt = this.getMTable(newTable.getDbName(), newTable.getTableName());
      if(newt != null) {
        if(!newTable.getTableName().equals(name) || !newTable.getDbName().equals(dbname)) {
          // if the old table and new table aren't the same
          throw new InvalidObjectException("new table " + newTable.getDbName() +" already exists");
        }
      }
      newt = convertToMTable(newTable);
      if(newt == null) {
        throw new InvalidObjectException("new table is invalid");
      }
      
      MTable oldt = this.getMTable(dbname, name);
      if(oldt == null) {
        throw new MetaException("table " + name + " doesn't exist");
      }
      
      // For now only alter name, owner, paramters, cols, bucketcols are allowed
      oldt.setTableName(newt.getTableName().toLowerCase());
      oldt.setParameters(newt.getParameters());
      oldt.setOwner(newt.getOwner());
      oldt.setSd(newt.getSd());
      oldt.setDatabase(newt.getDatabase());
      oldt.setRetention(newt.getRetention());
      //oldt.setPartitionKeys(newt.getPartitionKeys()); //this should never be changed for hive 
      
      // commit the changes
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
        success = false;
      }
    }
  }
}
