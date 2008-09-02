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

// hadoop stuff
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;

public class DB {

  protected Path whRoot_;

  private String dbName_;
  private Configuration conf_;
  private Path whPath_;
  FileStore store_;

  protected static final Log LOG = LogFactory.getLog(MetaStore.LogKey);

  public Path getPath() { return whPath_; }


  static {
    //        whRoot_ = new Path(FileSystem.getDefaultUri(new Configuration())); - not till 0.17
  }

  public DB(String dbName, Configuration conf) throws UnknownDBException, MetaException {
    String whRootString =  HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
    if(whRootString == null) {
      throw new MetaException(HiveConf.ConfVars.METASTOREWAREHOUSE.varname + " is not set in the config");
    }

    whRoot_ = new Path(whRootString);
    conf_ = conf;
    dbName_ = dbName;
    whPath_ = dbName.equals("default") ? whRoot_ : new Path(whRoot_, dbName + ".db");
    store_ = new FileStore(conf); 

    // a little bit of error checking...
    try {
      FileSystem fs  = whPath_.getFileSystem(conf);
      if(fs.exists(whPath_) == false || store_.dbExists(dbName) == false) {
        throw new UnknownDBException("name=" + dbName + ",whPath=" + whPath_ + " " + fs.exists(whPath_) + "," + store_.dbExists(dbName));
      }
    } catch(IOException e) {
      throw new MetaException(e.getMessage());
    }
  }

  public Path getDefaultTablePath(String tableName) {
    return getDefaultTablePath(tableName, (String)null);
  }

  static public Path getDefaultTablePath(String tableName, Configuration conf)  {
    try {
      return new DB(MetaStore.DefaultDB, conf).getDefaultTablePath(tableName, (String)null);
    } catch(UnknownDBException e) {
      // impossible
      throw new RuntimeException(e);
    } catch(MetaException e) {
      // impossible
      throw new RuntimeException(e);
    }
  }


  public String getName() { return dbName_; }

  public Path getDefaultTablePath(String tableName, String partition) {
    Path ret = new Path(whPath_, tableName );
    if(partition != null) {
      ret = new Path(ret, partition);
    }
    return ret;
  }

  /**
   * getTables
   *
   * Looks at metastore directories
   *
   * @param tablePattern
   * @return the list of tables
   * @exception MetaException
   *
   */
  public ArrayList<String> getTables(String tablePattern) throws MetaException {
    return store_.getTablesByPattern(this, tablePattern);
  }

  public Table getTable(String tableName, boolean o_rdonly) throws MetaException, UnknownTableException {
    if(store_.tableExists(this.dbName_, tableName) == false) {
      throw new UnknownTableException(tableName + " doesn't exist in database " + dbName_);
    }
    return new Table(this, tableName, conf_, o_rdonly);
  }

  public boolean tableExists(String tableName) throws MetaException {
    return store_.tableExists(dbName_, tableName);
  }

  public static DB createDB(String name, Configuration conf) throws MetaException {
    try {
      if(name.equals("default") || MetaStoreUtils.validateName(name) == false) {
        throw new MetaException("invalid db name: " + name + " - allowed characters are \\w and _ and default is reserved");
      }
      new FileStore(conf).createDatabase(name);
      Path whRoot = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE), name + ".db");
      FileSystem fs = FileSystem.get(whRoot.toUri(), conf);
      fs.mkdirs(whRoot);
      return new DB(name, conf);
    } catch(Exception e) {
      throw new MetaException(e.getMessage());
    }
  }
};
