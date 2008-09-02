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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.SerDe;
import org.apache.hadoop.hive.serde.SerDeUtils;
import org.apache.hadoop.util.StringUtils;

public class MetaStoreUtils {

  protected static final Log LOG = LogFactory.getLog("hive.log");

  public static final String DEFAULT_DATABASE_NAME = "default";

  /**
   * printStackTrace
   *
   * Helper function to print an exception stack trace to the log and not stderr
   *
   * @param e the exception
   *
   */
  static public void printStackTrace(Exception e) {
    for(StackTraceElement s: e.getStackTrace()) {
      LOG.error(s);
    }
  }

  public static Table createColumnsetSchema(String name, List<String> columns, List<String> partCols, Configuration conf) throws MetaException {

    if (columns == null) {
      throw new MetaException("columns not specified for table " + name);
    }
    
    Table tTable = new Table();
    tTable.setTableName(name);
    tTable.setSd(new StorageDescriptor());
    tTable.getSd().setSerdeInfo(new SerDeInfo());
    tTable.getSd().getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe.class.getName());
    tTable.getSd().getSerdeInfo().setSerializationFormat("1");
    
    List<FieldSchema>  fields = new ArrayList<FieldSchema>();
    tTable.getSd().setCols(fields);
    for (String col: columns) {
      FieldSchema field = new FieldSchema(col, Constants.STRING_TYPE_NAME, "default string type");
      fields.add(field);
    }

    tTable.setPartitionKeys(new ArrayList<FieldSchema>());
    for (String partCol : partCols) {
      FieldSchema part = new FieldSchema();
      part.setName(partCol);
      part.setType(Constants.STRING_TYPE_NAME); // default partition key
      tTable.getPartitionKeys().add(part);
    }
    // not sure why these are needed
    tTable.getSd().getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe.shortName());
    tTable.getSd().setNumBuckets(-1);
    return tTable;
  }


  /**
   * recursiveDelete
   *
   * just recursively deletes a dir - you'd think Java would have something to do this??
   *
   * @param f - the file/dir to delete
   * @exception IOException propogate f.delete() exceptions
   *
   */
  static public void recursiveDelete(File f) throws IOException {
    if(f.isDirectory()) {
      File fs [] = f.listFiles();
      for(File subf: fs) {
        recursiveDelete(subf);
      }
    }
    if(!f.delete()) {
      throw new IOException("could not delete: " + f.getPath());
    }
  }


  /**
   * getSerDe
   *
   * Get the SerDe for a table given its name and properties.
   *
   * @param name the name of the table
   * @param conf - hadoop config
   * @param p - the properties to use to instantiate the schema
   * @return the SerDe
   * @exception MetaException if any problems instantiating the serde
   *
   * todo - this should move somewhere into serde.jar
   *
   */
  static public SerDe getSerDe(Configuration conf, Properties schema) throws MetaException  {
    String lib = schema.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB);
    try {
      SerDe serDe = SerDeUtils.lookupSerDe(lib);
      ((SerDe)serDe).initialize(conf, schema);
      return serDe;
    } catch (Exception e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " " + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  /**
   * getSerDe
   *
   * Get the SerDe for a table given its name and properties.
   *
   * @param name the name of the table
   * @param conf - hadoop config
   * @param p - SerDe info
   * @return the SerDe
   * @exception MetaException if any problems instantiating the serde
   *
   * todo - this should move somewhere into serde.jar
   *
   */
  static public SerDe getSerDe(Configuration conf, org.apache.hadoop.hive.metastore.api.Table table) throws MetaException  {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    try {
      SerDe serDe = SerDeUtils.lookupSerDe(lib);
      ((SerDe)serDe).initialize(conf, MetaStoreUtils.getSchema(table));
      return serDe;
    } catch (Exception e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " " + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }
  
  static public void deleteWHDirectory(Path path,Configuration conf, boolean use_trash) throws MetaException {

    try {
      if(!path.getFileSystem(conf).exists(path)) {
        LOG.warn("drop data called on table/partition with no directory: " + path);
        return;
      }

      if(use_trash) {

        int count = 0;
        Path newPath = new Path("/Trash/Current" + path.getParent().toUri().getPath());

        if(path.getFileSystem(conf).exists(newPath) == false) {
          path.getFileSystem(conf).mkdirs(newPath);
        }

        do {
          newPath = new Path("/Trash/Current" + path.toUri().getPath() + "." + count);
          if(path.getFileSystem(conf).exists(newPath)) {
            count++;
            continue;
          }
          if(path.getFileSystem(conf).rename(path, newPath)) {
            break;
          }
        } while(++count < 50) ;
        if(count >= 50) {
          throw new MetaException("Rename failed due to maxing out retries");
        }
      } else {
        // directly delete it
        path.getFileSystem(conf).delete(path, true);
      }
    } catch(IOException e) {
      LOG.error("Got exception trying to delete data dir: " + e);
      throw new MetaException(e.getMessage());
    } catch(MetaException e) {
      LOG.error("Got exception trying to delete data dir: " + e);
      throw e;
    }
  }


  /**
   * validateTableName
   *
   * Checks the name conforms to our standars which are: "[a-zA-z-_.0-9]+".
   * checks this is just characters and numbers and _ and . and -
   *
   * @param tableName the name to validate
   * @return none
   * @exception MetaException if it doesn't match the pattern.
   */
  static public boolean validateName(String name) {
    Pattern tpat = Pattern.compile("[\\w_]+");
    Matcher m = tpat.matcher(name);
    if(m.matches()) {
      return true;
    }
    return false;
  }

  /**
   * Change from old to new format properties of a schema file
   *
   * @param p - a schema
   * @return the modified schema
   *
   */
  public static Properties hive1Tohive3ClassNames(Properties p) {
    for(Enumeration<?> e = p.propertyNames(); e.hasMoreElements() ; ) {
      String key = (String)e.nextElement();
      String oldName = p.getProperty(key);

      oldName = oldName.replace("com.facebook.infrastructure.tprofiles","com.facebook.serde.tprofiles");

      oldName = oldName.replace("com.facebook.infrastructure.hive_context","com.facebook.serde.hive_context");

      oldName = oldName.replace("com.facebook.thrift.hive.MetadataTypedColumnsetSerDe",org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe.class.getName());

      // columnset serde
      oldName = oldName.replace("com.facebook.thrift.hive.columnsetSerDe",org.apache.hadoop.hive.serde.thrift.columnsetSerDe.class.getName());

      // thrift serde
      oldName = oldName.replace("com.facebook.thrift.hive.ThriftHiveSerDe",org.apache.hadoop.hive.serde.thrift.ThriftSerDe.class.getName());

      p.setProperty(key,oldName);
    }
    return p;
  }

  public static String getListType(String t) {
    return "array<" + t + ">";
  }

  public static String getMapType(String k, String v) {
    return "map<" + k +"," + v + ">";
  }

  public static Table getTable(Properties schema) {
    Table t = new Table();
    t.setSd(new StorageDescriptor());
    t.setTableName(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME));
    t.getSd().setLocation(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION));
    t.getSd().setInputFormat(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
          org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName())); 
    t.getSd().setOutputFormat(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
          org.apache.hadoop.mapred.SequenceFileOutputFormat.class.getName())); 
    t.setPartitionKeys(new ArrayList<FieldSchema>());
    t.setDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    String part_cols_str = schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    t.setPartitionKeys(new ArrayList<FieldSchema>());
    if (part_cols_str != null && (part_cols_str.trim().length() != 0)) {
      String [] part_keys = part_cols_str.trim().split("/");
      for (String key: part_keys) {
        FieldSchema part = new FieldSchema();
        part.setName(key);
        part.setType(Constants.STRING_TYPE_NAME); // default partition key
        t.getPartitionKeys().add(part);
      }
    }
    t.getSd().setNumBuckets(Integer.parseInt(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.BUCKET_COUNT, "-1")));
    String bucketFieldName = schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.BUCKET_FIELD_NAME);
    t.getSd().setBucketCols(new ArrayList<String>(1));
    if ((bucketFieldName != null) && (bucketFieldName.trim().length() != 0)) {
      t.getSd().setBucketCols(new ArrayList<String>(1));
      t.getSd().getBucketCols().add(bucketFieldName);
    }
    
    t.getSd().setSerdeInfo(new SerDeInfo());
    t.getSd().getSerdeInfo().setName(t.getTableName());
    t.getSd().getSerdeInfo().setSerializationClass(schema.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS)); 
    t.getSd().getSerdeInfo().setSerializationFormat(schema.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT)); 
    t.getSd().getSerdeInfo().setSerializationLib(schema.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB));
    if(t.getSd().getSerdeInfo().getSerializationClass() == null || (t.getSd().getSerdeInfo().getSerializationClass().length() == 0)) {
      t.getSd().getSerdeInfo().setSerializationClass(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_SERDE));
    }
    
    String colstr = schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS);
    List<FieldSchema>  fields = new ArrayList<FieldSchema>();
    t.getSd().setCols(fields);
    if(colstr != null) {
      String[] cols =  colstr.split(",");
      for (String colName : cols) {
        FieldSchema col = new FieldSchema(colName, Constants.STRING_TYPE_NAME, "default string type");
        fields.add(col);
      }
    } 
    
    if(fields.size() == 0) {
      fields.add(new FieldSchema("__SERDE__", t.getSd().getSerdeInfo().getSerializationLib(), ""));
    }
    
    // remove all the used up parameters to find out the remaining parameters
    schema.remove(Constants.META_TABLE_NAME);
    schema.remove(Constants.META_TABLE_LOCATION);
    schema.remove(Constants.FILE_INPUT_FORMAT);
    schema.remove(Constants.FILE_OUTPUT_FORMAT);
    schema.remove(Constants.META_TABLE_PARTITION_COLUMNS);
    schema.remove(Constants.BUCKET_COUNT);
    schema.remove(Constants.BUCKET_FIELD_NAME);
    schema.remove(Constants.SERIALIZATION_CLASS);
    schema.remove(Constants.SERIALIZATION_FORMAT);
    schema.remove(Constants.SERIALIZATION_LIB);
    schema.remove(Constants.META_TABLE_SERDE);
    schema.remove(Constants.META_TABLE_COLUMNS);
    
    // add the remaining unknown parameters to the table's parameters
    t.setParameters(new HashMap<String, String>());
    for(Entry<Object, Object> e : schema.entrySet()) {
     t.getParameters().put(e.getKey().toString(), e.getValue().toString()); 
    }

    return t;
  }

  public static Properties getSchema(org.apache.hadoop.hive.metastore.api.Table tbl) {
    Properties schema = new Properties();
    String inputFormat = tbl.getSd().getInputFormat();
    if(inputFormat == null || inputFormat.length() == 0) {
      inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName();
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT, inputFormat);
    String outputFormat = tbl.getSd().getOutputFormat();
    if(outputFormat == null || outputFormat.length() == 0) {
      outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class.getName();
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT, outputFormat);
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, tbl.getTableName());
    if(tbl.getSd().getLocation() != null) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION, tbl.getSd().getLocation());
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.BUCKET_COUNT, Integer.toString(tbl.getSd().getNumBuckets()));
    if (tbl.getSd().getBucketCols().size() > 0) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.BUCKET_FIELD_NAME, tbl.getSd().getBucketCols().get(0));
    }
    if(tbl.getSd().getSerdeInfo().getSerializationClass() != null) {
      schema.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS, tbl.getSd().getSerdeInfo().getSerializationClass());
    }
    if(tbl.getSd().getSerdeInfo().getSerializationFormat() != null) {
      schema.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, tbl.getSd().getSerdeInfo().getSerializationFormat());
    }
    if(tbl.getSd().getSerdeInfo().getSerializationLib() != null) {
      schema.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB, tbl.getSd().getSerdeInfo().getSerializationLib());
    }
    StringBuffer buf = new StringBuffer();
    boolean first = true;
    for (FieldSchema col: tbl.getSd().getCols()) {
      if (!first) {
        buf.append(",");
      }
      buf.append(col.getName());
      first = false;
    }
    String cols = buf.toString();
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS, cols);
    
    String partString = "";
    String partStringSep = "";
    for (FieldSchema partKey : tbl.getPartitionKeys()) {
      partString = partString.concat(partStringSep);
      partString = partString.concat(partKey.getName());
      if(partStringSep.length() == 0) {
        partStringSep = "/";
      }
    }
    if(partString.length() > 0) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS, partString);
    }
    
    //TODO:pc field_to_dimension doesn't seem to be used anywhere so skipping for now
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.BUCKET_FIELD_NAME, "");
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.FIELD_TO_DIMENSION, "");
    
    for(Entry<String, String> e: tbl.getParameters().entrySet()) {
      schema.setProperty(e.getKey(), e.getValue());
    }
    
    return schema;
  }
  
  public static void makeDir(Path path, HiveConf hiveConf) throws MetaException {
    FileSystem fs;
    try {
      fs = path.getFileSystem(hiveConf);
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
    } catch (IOException e) {
      throw new MetaException("Unable to : " + path);
    } 

  }

  /**
   * Catches exceptions that can't be handled and bundles them to MetaException
   * @param e
   * @throws MetaException
   */
  static void logAndThrowMetaException(Exception e) throws MetaException {
    LOG.error("Got exception: " + e.getClass().getName() + " " + e.getMessage());
    LOG.error(StringUtils.stringifyException(e));
    throw new MetaException("Got exception: " + e.getClass().getName() + " " + e.getMessage());
  }
}
