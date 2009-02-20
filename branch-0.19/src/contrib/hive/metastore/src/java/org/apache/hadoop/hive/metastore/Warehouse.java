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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * This class represents a warehouse where data of Hive tables is stored
 */
public class Warehouse {
  private Path whRoot;
  private FileSystem fs;

  public static final Log LOG = LogFactory.getLog("hive.metastore.warehouse");

  public Warehouse(Configuration conf) throws MetaException {
    String whRootString =  HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
    if(StringUtils.isBlank(whRootString)) {
      throw new MetaException(HiveConf.ConfVars.METASTOREWAREHOUSE.varname + " is not set in the config or blank");
    }
    whRoot = new Path(whRootString);
    try {
      fs  = whRoot.getFileSystem(conf);
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
  }
  
  public FileSystem getFs() {
    return fs;
  }

  public Path getDefaultDatabasePath(String dbName) {
    if(dbName.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
      return whRoot;
    }
    return new Path(this.whRoot, dbName.toLowerCase() + ".db");
  }
  
  public Path getDefaultTablePath(String dbName, String tableName) {
    return new Path(getDefaultDatabasePath(dbName), tableName.toLowerCase());
  }

  public boolean mkdirs(Path f) throws MetaException {
    try {
      LOG.debug("Creating directory if it doesn't exist: " + f);
      return (fs.mkdirs(f) || fs.getFileStatus(f).isDir());
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }
  
  public boolean deleteDir(Path f, boolean recursive) throws MetaException {
    LOG.info("deleting  " + f);
    try {
      if(!fs.exists(f)) {
        return false;
      }
      Trash trashTmp = new Trash(fs.getConf());
      if (trashTmp.moveToTrash(f)) {
        LOG.info("Moved to trash: " + f);
        return true;
      }
      if (fs.delete(f, true)) {
        LOG.info("Deleted the diretory " + f);
        return true;
      }
      if(fs.exists(f)) {
        throw new MetaException("Unable to delete directory: " + f);
      }
    } catch (FileNotFoundException e) {
      return true; //ok even if there is not data
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

  public static String makePartName(LinkedHashMap<String, String> spec) throws MetaException {
    StringBuffer suffixBuf = new StringBuffer();
    for(Entry<String, String> e: spec.entrySet()) {
      if(e.getValue() == null  || e.getValue().length() == 0) {
        throw new MetaException("Partition spec is incorrect. " + spec);
      }
      suffixBuf.append(e.getKey() + "=" + e.getValue() + "/");
    }
    return suffixBuf.toString();
  }

  public Path getPartitionPath(String dbName, String tableName, LinkedHashMap<String, String> pm) throws MetaException {
    return new Path(getDefaultTablePath(dbName, tableName), makePartName(pm)); 
  }
  
  public Path getPartitionPath(Path tblPath, LinkedHashMap<String, String> pm) throws MetaException {
    return new Path(tblPath, makePartName(pm)); 
  }
  
  public boolean isDir(Path f) throws MetaException {
    try {
      FileStatus fstatus = fs.getFileStatus(f);
      if(!fstatus.isDir()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return true;
  }

  public static String makePartName(List<FieldSchema> partCols, List<String> vals) throws MetaException {
    if ((partCols.size() != vals.size()) || (partCols.size() == 0)) {
      throw new MetaException("Invalid partition key & values");
    }
    StringBuilder name = new StringBuilder();
    for(int i=0; i< partCols.size(); i++) {
      if(i > 0) {
        name.append('/');
      }
      name.append((partCols.get(i)).getName());
      name.append('=');
      name.append(vals.get(i));
    }
    return name.toString();
  }

}
