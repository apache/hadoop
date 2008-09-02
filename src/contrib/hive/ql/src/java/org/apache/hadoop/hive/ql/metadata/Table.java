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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.SerDe;
import org.apache.hadoop.hive.serde.SerDeField;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;


/**
 * A Hive Table: is a fundamental unit of data in Hive that shares a common schema/DDL
 */
public class Table {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Table");

  private Properties schema;
  private SerDe serDe;
  private URI uri;
  private Class<? extends InputFormat> inputFormatClass;
  private Class<? extends OutputFormat> outputFormatClass;
  private org.apache.hadoop.hive.metastore.api.Table tTable;

  /**
   * Table (only used internally)
   * @throws HiveException 
   *
   */
  protected Table() throws HiveException {
  }

  /**
   * Table
   *
   * Create a TableMetaInfo object presumably with the intent of saving it to the metastore
   *
   * @param name the name of this table in the metadb
   * @param schema an object that represents the schema that this SerDe must know
   * @param serDe a Class to be used for serializing and deserializing the data
   * @param dataLocation where is the table ? (e.g., dfs://hadoop001.sf2p.facebook.com:9000/user/facebook/warehouse/example) NOTE: should not be hardcoding this, but ok for now
   *
   * @exception HiveException on internal error. Note not possible now, but in the future reserve the right to throw an exception
   */
  public Table(String name, Properties schema, SerDe serDe, 
      Class<? extends InputFormat<?, ?>> inputFormatClass,
      Class<? extends OutputFormat<?, ?>> outputFormatClass,
      URI dataLocation, Hive hive) throws HiveException {
    initEmpty();
    this.getTTable().setTableName(name);
    this.getTTable().getSd().setLocation(dataLocation.toASCIIString());
    this.setInputFormatClass(inputFormatClass);
    this.setOutputFormatClass(outputFormatClass);
    this.setDataLocation(dataLocation);
    this.schema = schema;
    this.serDe = serDe; //TODO: convert to SerDeInfo format
    this.getTTable().getSd().getSerdeInfo().setSerializationLib(serDe.getClass().getName());
  }
  
  public Table(String name) {
    // fill in defaults
    initEmpty();
    this.getTTable().setTableName(name);
    this.getTTable().setDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    this.getTTable().getSd().getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe.class.getName());
    this.getTTable().getSd().getSerdeInfo().setSerializationFormat("1");
  }
  
  void initEmpty() {
    this.setTTable(new org.apache.hadoop.hive.metastore.api.Table());
    this.getTTable().setSd(new StorageDescriptor());
    this.getTTable().getSd().setSerdeInfo(new SerDeInfo());
    this.getTTable().getSd().setNumBuckets(-1);
    this.getTTable().getSd().setBucketCols(new ArrayList<String>());
    this.getTTable().getSd().setCols(new ArrayList<FieldSchema>());
    this.getTTable().setPartitionKeys(new ArrayList<FieldSchema>());
    this.getTTable().setParameters(new HashMap<String, String>());
    this.getTTable().getSd().setParameters(new HashMap<String, String>());
    this.getTTable().getSd().setSortCols(new ArrayList<Order>());
  }
  
  protected void initSerDe() throws HiveException {
    if(this.serDe == null) {
      try {
        this.serDe = MetaStoreUtils.getSerDe(Hive.get().getConf(), this.getTTable());
      } catch (MetaException e) {
        throw new HiveException(e);
      }
    }
  }
  public void checkValidity() throws HiveException {

    // check for validity
    String name = this.getTTable().getTableName();
    if (null == name || name.length() == 0 || !MetaStoreUtils.validateName(name)) {
      throw new HiveException("[" + name + "]: is not a valid table name");
    }
    if (null == this.getSerDe()) {
      throw new HiveException("must specify a non-null serDe");
    }
    if (null == this.getInputFormatClass()) {
      throw new HiveException("must specify an InputFormat class");
    }
    if (null == this.getOutputFormatClass()) {
      throw new HiveException("must specify an OutputFormat class");
    }
    return;
  }

  /**
   * @param inputFormatClass the inputFormatClass to set
   */
  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    tTable.getSd().setInputFormat(inputFormatClass.getName());
  }

  /**
   * @param outputFormatClass the outputFormatClass to set
   */
  public void setOutputFormatClass(Class<? extends OutputFormat> outputFormatClass) {
    this.outputFormatClass = outputFormatClass;
    tTable.getSd().setOutputFormat(outputFormatClass.getName());
  }

  final public Properties getSchema()  {
    return this.schema;
  }

  final public Path getPath() {
    return new Path(this.getTTable().getSd().getLocation());
  }

  final public String getName() {
    return this.getTTable().getTableName();
  }

  final public URI getDataLocation() {
    return this.uri;
  }

  final public SerDe getSerDe() {
    return this.serDe;
  }

  final public Class<? extends InputFormat> getInputFormatClass() {
    return this.inputFormatClass;
  }

  final public Class<? extends OutputFormat> getOutputFormatClass() {
    return this.outputFormatClass;
  }

  final public boolean isValidSpec(AbstractMap<String, String> spec) throws HiveException {

    // TODO - types need to be checked.
    List<FieldSchema> partCols = this.getTTable().getPartitionKeys();
    if(partCols== null || (partCols.size() == 0)) {
      if (spec != null)
        throw new HiveException("table is not partitioned but partition spec exists: " + spec);
      else
        return true;
    }
    
    if((spec == null) || (spec.size() != partCols.size())) {
      throw new HiveException("table is partitioned but partition spec is not specified or tab: " + spec);
    }
    
    for (FieldSchema field : partCols) {
      if(spec.get(field.getName()) == null) {
        throw new HiveException(field.getName() + " not found in table's partition spec: " + spec);
      }
    }

    return true;
  }
  
  public void setProperty(String name, String value) {
    this.getTTable().getParameters().put(name, value);
  }

  /**
   * getProperty
   *
   */
  public String getProperty(String name) {
    return this.getTTable().getParameters().get(name);
  }

  public Vector<SerDeField> getFields(String [] components) {


    Vector<SerDeField> fields = new Vector<SerDeField> ();
    try {
      SerDe decoder = getSerDe();
      if (components == null || components.length == 0) {
        // Expand out all the columns of the table
        List<SerDeField> fld_lst = decoder.getFields(null);
        for(SerDeField field: fld_lst) {
          fields.add(field);
        }
      }
      else {
        for (int i = 0; i < components.length; i++) {
          fields.add(decoder.getFieldFromExpression(null, components[i]));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return fields;
  }

  public SerDeField getField(String fld) {
    try {
      return getSerDe().getFieldFromExpression(null, fld);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @param schema the schema to set
   */
  public void setSchema(Properties schema) {
    this.schema = schema;
  }

  /**
   * @param serDe the serDe to set
   */
  public void setSerDe(SerDe serDe) {
    this.serDe = serDe;
  }

  public String toString() { 
    return this.getTTable().getTableName();
  }

  public List<FieldSchema> getPartCols() {
    List<FieldSchema> partKeys = this.getTTable().getPartitionKeys();
    if(partKeys == null) {
      partKeys = new ArrayList<FieldSchema>();
      this.getTTable().setPartitionKeys(partKeys);
    }
    return partKeys;
  }
  
  public boolean isPartitionKey(String colName) {
    for (FieldSchema key : this.getPartCols()) {
      if(key.getName().toLowerCase().equals(colName)) {
        return true;
      }
    }
    return false;
  }

  //TODO merge this with getBucketCols function
  public String getBucketingDimensionId() {
    List<String> bcols = this.getTTable().getSd().getBucketCols();
    if(bcols == null || bcols.size() == 0) {
      return null;
    }
    
    if(bcols.size() > 1) {
      LOG.warn(this + " table has more than one dimensions which aren't supported yet");
    }
    
    return bcols.get(0);
  }

  public String getSerializationFormat() {
    return this.getTTable().getSd().getSerdeInfo().getSerializationFormat();
  }

  public void setSerializationFormat(String f) {
    this.getTTable().getSd().getSerdeInfo().setSerializationFormat(f);
  }

  /**
   * @return the tTable
   */
  public org.apache.hadoop.hive.metastore.api.Table getTTable() {
    return tTable;
  }

  /**
   * @param table the tTable to set
   */
  protected void setTTable(org.apache.hadoop.hive.metastore.api.Table table) {
    tTable = table;
  }

  public void setDataLocation(URI uri2) {
    this.uri = uri2;
    this.getTTable().getSd().setLocation(uri2.toString());
  }

  public void setSerializationClass(String cls) {
    this.getTTable().getSd().getSerdeInfo().setSerializationClass(cls);
  }

  public void setSerializationLib(String lib) {
    this.getTTable().getSd().getSerdeInfo().setSerializationLib(lib);
  }

  public void setBucketCols(List<String> bucketCols) throws HiveException {
    if (bucketCols == null) {
      return;
    }

    for (String col : bucketCols) {
      if(!this.isField(col))
        throw new HiveException("Bucket columns " + col + " is not part of the table columns" ); 
    }
    this.getTTable().getSd().setBucketCols(bucketCols);
  }

  public void setSortCols(List<String> sortCols) throws HiveException {
    List<Order> sortOrder = new ArrayList<Order>();
    for (String col : sortCols) {
      sortOrder.add(new Order(col, 1));
    }
    this.getTTable().getSd().setSortCols(sortOrder);
  }

  private boolean isField(String col) {
    for (FieldSchema field : this.getCols()) {
      if(field.getName().equals(col)) {
        return true;
      }
    }
    return false; 
  }

  public List<FieldSchema> getCols() {
    return this.getTTable().getSd().getCols();
  }

  public void setPartCols(List<FieldSchema> partCols) {
    this.getTTable().setPartitionKeys(partCols);
  }

  public String getDbName() {
    return this.getTTable().getDatabase();
  }

  public int getNumBuckets() {
    return this.getTTable().getSd().getNumBuckets();
  }
  
  /**
   * Replaces files in the partition with new data set specifed by srcf. Works by moving files
   *
   * @param srcf Files to be moved. Leaf Directories or Globbed File Paths
   */
  protected void replaceFiles(Path srcf) throws HiveException {
    FileSystem fs;
    try {
      fs = FileSystem.get(this.getDataLocation(), Hive.get().getConf());
      Hive.get().replaceFiles(srcf, new Path(this.getDataLocation().getPath()), fs);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
  }

  /**
   * Inserts files specified into the partition. Works by moving files
   *
   * @param srcf Files to be moved. Leaf Directories or Globbed File Paths
   */
  protected void copyFiles(Path srcf) throws HiveException {
    FileSystem fs;
    try {
      fs = FileSystem.get(this.getDataLocation(), Hive.get().getConf());
      Hive.get().copyFiles(srcf, new Path(this.getDataLocation().getPath()), fs);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
  }

  public void setInputFormatClass(String name) throws HiveException {
    try {
      this.setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>)Class.forName(name));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public void setOutputFormatClass(String name) throws HiveException {
    try {
      this.setOutputFormatClass((Class<? extends OutputFormat<WritableComparable, Writable>>)Class.forName(name));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  
  public boolean isPartitioned() {
    if(this.getPartCols() == null) {
      return false;
    }
    return (this.getPartCols().size() != 0);
  }

  public void setFields(List<FieldSchema> fields) {
    this.getTTable().getSd().setCols(fields);
  }

  public void setNumBuckets(int nb) {
    this.getTTable().getSd().setNumBuckets(nb);
  }

  /**
   * @return
   * @see org.apache.hadoop.hive.metastore.api.Table#getOwner()
   */
  public String getOwner() {
    return tTable.getOwner();
  }

  /**
   * @return
   * @see org.apache.hadoop.hive.metastore.api.Table#getParameters()
   */
  public Map<String, String> getParameters() {
    return tTable.getParameters();
  }

  /**
   * @return
   * @see org.apache.hadoop.hive.metastore.api.Table#getRetention()
   */
  public int getRetention() {
    return tTable.getRetention();
  }

  /**
   * @param owner
   * @see org.apache.hadoop.hive.metastore.api.Table#setOwner(java.lang.String)
   */
  public void setOwner(String owner) {
    tTable.setOwner(owner);
  }

  /**
   * @param retention
   * @see org.apache.hadoop.hive.metastore.api.Table#setRetention(int)
   */
  public void setRetention(int retention) {
    tTable.setRetention(retention);
  }

  public String getSerializationLib() {
    return this.getTTable().getSd().getSerdeInfo().getSerializationLib();
  }

  public String getSerializationClass() {
    return this.getTTable().getSd().getSerdeInfo().getSerializationClass();
  }

  public void setIsCompressed(boolean b) {
    this.getTTable().getSd().setIsCompressed(b);
  }

  public void setFieldDelim(String string) {
    this.getTTable().getSd().getSerdeInfo().setFieldDelim(string);  
  }

  public void setCollectionItemDelim(String string) {
    this.getTTable().getSd().getSerdeInfo().setCollectionItemDelim(string);  
  }

  public void setLineDelim(String string) {
    this.getTTable().getSd().getSerdeInfo().setLineDelim(string);  
  }

  public void setMapKeyDelim(String string) {
    this.getTTable().getSd().getSerdeInfo().setMapKeyDelim(string);  
  }
  
  public String getFieldDelim() {
    return this.getTTable().getSd().getSerdeInfo().getFieldDelim();
  }

  public String getCollectionItemDelim() {
    return this.getTTable().getSd().getSerdeInfo().getCollectionItemDelim();
  }
  
  public String getLineDelim() {
    return this.getTTable().getSd().getSerdeInfo().getLineDelim();
  }
  
  public String getMapKeyDelim() {
    return this.getTTable().getSd().getSerdeInfo().getMapKeyDelim();
  }

  public List<String> getBucketCols() {
    return this.getTTable().getSd().getBucketCols();
  }

  public List<Order> getSortCols() {
    return this.getTTable().getSd().getSortCols();
  }

  private static void getPartPaths(FileSystem fs, Path p, Vector<String> partPaths) throws IOException {
    // Base case for recursion
    if (fs.isFile(p)) {
      if (!partPaths.contains(p.getParent().toString())) {
        partPaths.add(p.getParent().toString());
      }
    }
    else {
      FileStatus [] dirs = fs.listStatus(p);

      if (dirs.length != 0 ) {
        for(int i=0; i < dirs.length; ++i) {
          getPartPaths(fs, dirs[i].getPath(), partPaths);
        }
      }
      else {
        // This is an empty partition
        if (!partPaths.contains(p.toString())) {
          partPaths.add(p.toString());
        }
      }
    }

    return;
  }

  static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");
  protected List<Partition> getPartitionsFromHDFS() throws HiveException {
    ArrayList<Partition> ret = new ArrayList<Partition> ();
    FileSystem fs = null;
    Vector<String> partPaths = new Vector<String>();

    try {
      fs = FileSystem.get(this.getDataLocation(), Hive.get().getConf());
      getPartPaths(fs, new Path(this.getDataLocation().getPath()), partPaths);
      for(String partPath: partPaths) {
        Path tmpPath = new Path(partPath);
        if(!fs.getFileStatus(tmpPath).isDir()) {
          throw new HiveException("Data in hdfs is messed up. Table " + getName() + " has a partition " + partPath + " that is not a directory");
        }
        ret.add(new Partition(this, tmpPath));
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("DB Error: Table " + this.getDataLocation() + " message: " + e.getMessage());
    }

    return ret;
  }
  
  
};
