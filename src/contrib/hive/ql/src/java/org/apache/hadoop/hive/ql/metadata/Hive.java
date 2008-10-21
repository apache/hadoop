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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;

import com.facebook.thrift.TException;

/**
 * The Hive class contains information about this instance of Hive.
 * An instance of Hive represents a set of data in a file system (usually HDFS)
 * organized for easy query processing
 *
 */

public class Hive {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Hive");

  private HiveConf conf;
  private IMetaStoreClient msc;

  static Hive db = null;
  
  /**
   * get
   *
   * @param c
   * @return
   * @exception
   *
   */
  public static Hive get(HiveConf c) throws HiveException {
    if(db == null) {
      // TODO - this doesn't work if the user switches to a different metadb server
      // during the same session. revisit.
      c.set("fs.scheme.class","dfs");
      db = new Hive(c);
    }
    return db;
  }

  public static Hive get() throws HiveException {
    if(db == null) {
      db = new Hive(new HiveConf(ParseDriver.class));
    }
    return db;
  }
  
  /**
   * Hive
   *
   * @param argFsRoot
   * @param c
   *
   */
  private Hive(HiveConf c) throws  HiveException {
    this.conf = c;
    try {
       msc = this.createMetaStoreClient();
    } catch (MetaException e) {
      throw new HiveException("Unable to open connection to metastore", e);
    }
  }
  
  public static void closeCurrent() {
    if(Hive.db != null) {
      LOG.info("Closing current connection to Hive Metastore.");
      if(db.msc != null) {
        db.msc.close();
      }
      db.msc = null;
      db = null;
    }
  }
  
  /**
   * Creates a table metdata and the directory for the table data
   * @param tableName name of the table
   * @param columns list of fields of the table
   * @param partCols partition keys of the table
   * @param fileInputFormat Class of the input format of the table data file
   * @param fileOutputFormat Class of the output format of the table data file
   * @throws HiveException thrown if the args are invalid or if the metadata or the data directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns, List<String> partCols,
      Class<? extends InputFormat> fileInputFormat, Class<? extends OutputFormat> fileOutputFormat) throws HiveException {
    this.createTable(tableName, columns, partCols, fileInputFormat, fileOutputFormat, -1, null);
  }

  /**
   * Creates a table metdata and the directory for the table data
   * @param tableName name of the table
   * @param columns list of fields of the table
   * @param partCols partition keys of the table
   * @param fileInputFormat Class of the input format of the table data file
   * @param fileOutputFormat Class of the output format of the table data file
   * @param bucketCount number of buckets that each partition (or the table itself) should be divided into
   * @throws HiveException thrown if the args are invalid or if the metadata or the data directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns, List<String> partCols,
      Class<? extends InputFormat> fileInputFormat, Class<? extends OutputFormat> fileOutputFormat, int bucketCount, List<String> bucketCols) throws HiveException {
    if(columns == null) {
      throw new HiveException("columns not specified for table " + tableName);
    }

    Table tbl = new Table(tableName);
    tbl.setInputFormatClass(fileInputFormat.getName());
    tbl.setOutputFormatClass(fileOutputFormat.getName());

    for (String col: columns) {
      FieldSchema field = new FieldSchema(col, org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME, "default");
      tbl.getCols().add(field);
    }

    if(partCols != null) {
      for (String partCol : partCols) {
        FieldSchema part = new FieldSchema();
        part.setName(partCol);
        part.setType(org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME); // default partition key
        tbl.getPartCols().add(part);
      }
    }
    tbl.setSerializationLib(org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class.getName());
    tbl.setNumBuckets(bucketCount);
    createTable(tbl);
  }


  /**
   * Updates the existing table metadata with the new metadata. 
   * @param tblName name of the existing table
   * @param newTbl new name of the table. could be the old name
   * @throws InvalidOperationException if the changes in metadata is not acceptable
   * @throws MetaException
   * @throws TException
   */
  public void alterTable(String tblName,
      Table newTbl) throws InvalidOperationException,
      MetaException, TException {
    msc.alter_table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName, newTbl.getTTable());
  }

  /**
   * Creates the table with the give objects
   * @param tbl a table object
   * @throws HiveException
   */
  public void createTable(Table tbl) throws HiveException {
    try {
      tbl.initSerDe();
      tbl.checkValidity();
      msc.createTable(tbl.getTTable());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it is a no-op
   * @param tableName
   * @throws HiveException
   */
  public void dropTable(String tableName) throws HiveException {
    dropTable(tableName, true, true);
  }

  /**
   * Drops the table. 
   * @param tableName
   * @param deleteData deletes the underlying data along with metadata
   * @param ignoreUnknownTab an exception if thrown if this is falser and table doesn't exist
   * @throws HiveException
   */
  public void dropTable(String tableName, boolean deleteData, boolean ignoreUnknownTab) throws HiveException {
    try {
      msc.dropTable(tableName, deleteData);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (UnknownTableException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    } 
  }

  public HiveConf getConf() {
    return (conf);
  }
  
  /**
   * Returns metadata of the table. 
   * @param tableName the name of the table
   * @return the table
   * @exception HiveException if there's an internal error or if the table doesn't exist 
   */
  public Table getTable(final String tableName) throws HiveException {
    return this.getTable(tableName, true);
  }
  
  /**
   * Returns metadata of the table
   * @param tableName
   * @param throwException controls whether an exception is thrown or a returns a null
   * @return
   * @throws HiveException
   */
  public Table getTable(final String tableName, boolean throwException) throws HiveException {

    if(tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }
    Table table = new Table();
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      tTable = msc.getTable(tableName);
    } catch (NoSuchObjectException e) {
      if(throwException) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidTableException("Table not found ", tableName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch table " + tableName, e);
    }
    // just a sanity check
    assert(tTable != null);
    try {
      // first get a schema (in key / vals)
      Properties p = MetaStoreUtils.getSchema(tTable);
      // Map hive1 to hive3 class names, can be removed when migration is done.
      p = MetaStoreUtils.hive1Tohive3ClassNames(p);
      table.setSchema(p);
      table.setTTable(tTable);
      table.setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>)
          Class.forName(table.getSchema().getProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
              org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName())));
      table.setOutputFormatClass((Class<? extends OutputFormat<WritableComparable, Writable>>)
          Class.forName(table.getSchema().getProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
              org.apache.hadoop.mapred.SequenceFileOutputFormat.class.getName()))); 
      table.setDeserializer(MetaStoreUtils.getDeserializer(getConf(), p));
      table.setDataLocation(new URI(tTable.getSd().getLocation()));
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    String sf = table.getSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if(sf != null) {
      char[] b = sf.toCharArray();
      if ((b.length == 1) && (b[0] < 10)){ // ^A, ^B, ^C, ^D, \t
        table.setSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, Integer.toString(b[0]));
      }
    }
    table.checkValidity();
    return table;
  }
  
  public List<String> getAllTables() throws HiveException {
    return getTablesByPattern(".*");
  }
  
  /**
   * returns all existing tables that match the given pattern. The matching occurs as per Java regular expressions
   * @param tablePattern java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String tablePattern) throws HiveException {
    try {
      return msc.getTables(MetaStoreUtils.DEFAULT_DATABASE_NAME, tablePattern);
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }
  
  // for testing purposes
  protected List<String> getTablesForDb(String database, String tablePattern) throws HiveException {
    try {
      return msc.getTables(database, tablePattern);
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }
  
  /**
   * @param name
   * @param location_uri
   * @return
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#createDatabase(java.lang.String, java.lang.String)
   */
  protected boolean createDatabase(String name, String locationUri) throws AlreadyExistsException,
      MetaException, TException {
    return msc.createDatabase(name, locationUri);
  }

  /**
   * @param name
   * @return
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDatabase(java.lang.String)
   */
  protected boolean dropDatabase(String name) throws MetaException, TException {
    return msc.dropDatabase(name);
  }

  /**
   * Load a directory into a Hive Table Partition
   * - Alters existing content of the partition with the contents of loadPath.
   * - If he partition does not exist - one is created
   * - files in loadPath are moved into Hive. But the directory itself is not removed.
   *
   * @param jc Job configuration
   * @param loadPath Directory containing files to load into Table
   * @param tableName name of table to be loaded.
   * @param partSpec defines which partition needs to be loaded
   * @param replace if true - replace files in the partition, otherwise add files to the partition
   */
  public void loadPartition(Path loadPath, String tableName,
      AbstractMap<String, String> partSpec, boolean replace)
  throws HiveException {
    Table tbl = getTable(tableName);
    Partition part = getPartition(tbl, partSpec, true);
    if(replace) {
      part.replaceFiles(loadPath);
    } else {
      part.copyFiles(loadPath);
    }
  }

  /**
   * Load a directory into a Hive Table.
   * - Alters existing content of table with the contents of loadPath.
   * - If table does not exist - an exception is thrown
   * - files in loadPath are moved into Hive. But the directory itself is not removed.
   *
   * @param jc Job configuration
   * @param loadPath Directory containing files to load into Table
   * @param tableName name of table to be loaded.
   * @param replace if true - replace files in the table, otherwise add files to table
   */
  public void loadTable(Path loadPath, String tableName, boolean replace) throws HiveException {
    Table tbl = getTable(tableName);
    if(replace) {
      tbl.replaceFiles(loadPath);
    } else {
      tbl.copyFiles(loadPath);
    }
  }
  
  /**
   * Creates a partition 
   * @param tbl table for which partition needs to be created
   * @param partSpec partition keys and their values
   * @return created partition object
   * @throws HiveException if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, AbstractMap<String, String> partSpec) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      pvals.add(partSpec.get(field.getName()));
    }
    try {
      tpart = msc.appendPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl.getName(), pvals);;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return new Partition(tbl, tpart);
  }

  /**
   * Returns partition metadata
   * @param tableName name of the partition's table
   * @param partSpec partition keys and values
   * @param forceCreate if this is true and partition doesn't exist then a partition is created
   * @return result partition object or null if there is no partition
   * @throws HiveException
   */
  public Partition getPartition(Table tbl, AbstractMap<String, String> partSpec, boolean forceCreate)
      throws HiveException {
    if(!tbl.isValidSpec(partSpec)) {
      throw new HiveException("Invalid partition: " + partSpec);
    }
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      if(val == null || val.length() == 0) {
        throw new HiveException("Value for key " + field.getName() + " is null or empty");
      }
      pvals.add(val);
    }
    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    try {
      tpart = msc.getPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl.getName(), pvals);
      if(tpart == null && forceCreate) {
        LOG.debug("creating partition for table "  + tbl.getName() + " with partition spec : " + partSpec);
        tpart = msc.appendPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl.getName(), pvals);;
      }
      if(tpart == null){
        return null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return new Partition(tbl, tpart);
  }
  
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData) throws HiveException {
    try {
      return msc.dropPartition(db_name, tbl_name, part_vals, deleteData);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException("Unknow error. Please check logs.", e);
    }
  }

  public List<String> getPartitionNames(String dbName, String tblName, short max) throws HiveException {
    List names = null;
    try {
      names = msc.listPartitionNames(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  /**
   * get all the partitions that the table has
   * @param tbl object for which partition is needed
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl) throws HiveException {
    if(tbl.isPartitioned()) {
      List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
      try {
        tParts = msc.listPartitions(tbl.getDbName(), tbl.getName(), (short) -1);
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
      List<Partition> parts = new ArrayList<Partition>(tParts.size());
      for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
        parts.add(new Partition(tbl, tpart));
      }
      // get the partitions on the HDFS location
      List<Partition> hdfsPartitions = tbl.getPartitionsFromHDFS();
      if(hdfsPartitions.size() != parts.size()) {
        // HACK: either we are connecting to old metastore or metadata is out of sync with data
        // TODO: for the former case, move this logic into OLD metastore and compare 
        // the two lists here for any conflict between metadata and data
        LOG.error("Metadata for partitions doesn't match the data in HDFS. Table name: " + tbl.getName());
        // let's trust hdfs partitions for now
        return hdfsPartitions;
      }
      return parts;
    } else {
      // create an empty partition. 
      // HACK, HACK. SemanticAnalyzer code requires that an empty partition when the table is not partitioned
      org.apache.hadoop.hive.metastore.api.Partition tPart = new org.apache.hadoop.hive.metastore.api.Partition();
      tPart.setSd(tbl.getTTable().getSd()); // TODO: get a copy
      Partition part = new Partition(tbl, tPart);
      ArrayList<Partition> parts = new ArrayList<Partition>(1);
      parts.add(part);
      return parts;
    }
  }

  public static boolean needsDeletion(FileStatus file) {
    String name = file.getPath().getName();
    // There is a race condition in hadoop as a result of which
    // the _task files created in the output directory at the time
    // of the mapper is reported in the output directory even though
    // it is actually removed. The first check works around that
    // NOTE: it's not clear that this still affects recent versions of hadoop

    // the second check deals with uncommitted output files produced by hive tasks
    // this would typically happen on task failures or due to speculation
    return (name.startsWith("_task") || name.startsWith("_tmp."));
  }

  private void checkPaths(FileSystem fs, FileStatus [] srcs, Path destf, boolean replace) throws HiveException {
    try {
        for(int i=0; i<srcs.length; i++) {
            FileStatus [] items = fs.listStatus(srcs[i].getPath());
            for(int j=0; j<items.length; j++) {

                if (needsDeletion(items[j])) {
                      fs.delete(items[j].getPath(), true);
                      continue;
                }
                if(items[j].isDir()) {
                    throw new HiveException("checkPaths: "+srcs[i].toString()+" has nested directory"+
                                            items[j].toString());
                }
                Path tmpDest = new Path(destf, items[j].getPath().getName());
                if(!replace && fs.exists(tmpDest)) {
                    throw new HiveException("checkPaths: " + tmpDest + " already exists");
                }
            }
        }
    } catch (IOException e) {
        throw new HiveException("checkPaths: filesystem error in check phase", e);
    }
}

  protected void copyFiles(Path srcf, Path destf, FileSystem fs) throws HiveException {
    FileStatus[] srcs;
    try {
      srcs = fs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      //srcs = new FileStatus[0]; Why is this needed?
    }
    // check that source and target paths exist
    checkPaths(fs, srcs, destf, false);

    // move it, move it
    try {
      for(int i=0; i<srcs.length; i++) {
        FileStatus [] items = fs.listStatus(srcs[i].getPath());
        for(int j=0; j<items.length; j++) {
          fs.rename(items[j].getPath(), new Path(destf, items[j].getPath().getName()));
        }
      }
    } catch (IOException e) {
      throw new HiveException("addFiles: error while moving files!!!", e);
    }
  }

  /**
   * Replaces files in the partition with new data set specifed by srcf. Works by moving files
   *
   * @param srcf Files to be moved. Leaf Directories or Globbed File Paths
   */
  protected void replaceFiles(Path srcf, Path destf, FileSystem fs) throws HiveException {
      FileStatus [] srcs;
      try {
          srcs = fs.globStatus(srcf);
      } catch (IOException e) {
          throw new HiveException("addFiles: filesystem error in check phase", e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
        //srcs = new FileStatus[0]; Why is this needed?
      }
      checkPaths(fs, srcs, destf, true);

      Random randGen = new Random();
      Path tmppath = new Path("/tmp/"+randGen.nextInt());
      try {
          fs.mkdirs(tmppath);
          for(int i=0; i<srcs.length; i++) {
              FileStatus[] items = fs.listStatus(srcs[i].getPath());
              for(int j=0; j<items.length; j++) {
                  boolean b = fs.rename(items[j].getPath(), new Path(tmppath, items[j].getPath().getName()));
                  LOG.debug("Renaming:"+items[j]+",Status:"+b);
              }
          }

          // point of no return
          boolean b = fs.delete(destf, true);
          LOG.debug("Deleting:"+destf.toString()+",Status:"+b);
          b = fs.rename(tmppath, destf);
          LOG.debug("Renaming:"+tmppath.toString()+",Status:"+b);

      } catch (IOException e) {
          throw new HiveException("replaceFiles: error while moving files!!!", e);
      } finally {
          try {
              fs.delete(tmppath, true);
          } catch (IOException e) {
            LOG.warn("Unable delete path " + tmppath, e);
          }
      }
  }

  /**
   * @return
   * @throws HiveMetaException 
   */
  private IMetaStoreClient createMetaStoreClient() throws MetaException {
    boolean useFileStore = conf.getBoolean("hive.metastore.usefilestore", false);
    if(!useFileStore) {
      return new HiveMetaStoreClient(this.conf);
    }
    return new MetaStoreClient(this.conf);
  }
  
};
