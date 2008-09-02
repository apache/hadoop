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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.util.StringUtils;


public class RWTable extends ROTable {

  // bugbug - make this a param
  boolean use_trash_ = false;

  protected RWTable() {
    // used internally for creates
  }

  protected boolean o_rdonly_;

  // not called directly -- use DB.getTable
  protected RWTable(DB parent, String tableName, Configuration conf, boolean o_rdonly) throws UnknownTableException, MetaException {
    super(parent, tableName, conf);
    this.o_rdonly_ = o_rdonly;
  }

  //    protected void finalize() {    }

  public void createPartition(String name) throws IOException {
    Path path = new Path(this.whPath_, name);
    path.getFileSystem(this.conf_).mkdirs(path);
  }


  /**
   * drop
   *
   * delete the schema for this table and optionally delete the data. Note the data is actually moved to the
   * Trash, not really deleted.
   *
   * @exception MetaException if any problems instantiating this object
   *
   */
  @SuppressWarnings("nls")
  public void drop() throws MetaException {

    if(this.o_rdonly_) {
      throw new RuntimeException("cannot perform write operation on a read-only table");
    }


    MetaStoreUtils.deleteWHDirectory(this.whPath_, this.conf_, this.use_trash_);
    try {
      this.store_.drop(this.parent_, this.tableName_);
    } catch(IOException e) {
      throw new MetaException(e.getMessage());
    }
    this.o_rdonly_ = true; // the table is dropped, so can only do reads now
  }

  /**
   * truncate
   *
   * delete the data, but not the schema
   * Can be applied on a partition by partition basis
   *
   * @param partition partition in that table or "" or null
   * @exception MetaException if any problems instantiating this object
   *
   */
  public void truncate() throws MetaException {
    truncate("");
  }

  @SuppressWarnings("nls")
  public void truncate(String partition) throws MetaException {

    if (this.o_rdonly_) {
      throw new RuntimeException("cannot perform write operation on a read-only table");
    }

    try {
      MetaStoreUtils.deleteWHDirectory((partition == null || partition.length() == 0) ? this.whPath_ : new Path(this.whPath_,partition), this.conf_, this.use_trash_);
      // ensure the directory is re-made
      if (partition == null || partition.length() == 0) {
        this.whPath_.getFileSystem(this.conf_).mkdirs(this.whPath_);
      }
    } catch(IOException e) {
      throw new MetaException(e.getMessage());
    }
  }

  /**
   * alter
   *
   * Add column names to a column set ser de table.
   *
   * @param tableName the name of the table to alter
   * @param columns the name of the columns
   * @exception MetaException if any problems altering the table
   *
   */
  @SuppressWarnings("nls")
  public void alter(Properties schema) throws MetaException  {
    if(this.o_rdonly_) {
      throw new RuntimeException("cannot perform write operation on a read-only table");
    }

    // check if a rename, in which case, we move the schema file and the data
    String newName = schema.getProperty(Constants.META_TABLE_NAME);
    String newLoc = schema.getProperty(Constants.META_TABLE_LOCATION);

    if(newName.equals(this.tableName_) == false) {
      // RENAME
      Path newPath = newLoc.equals(this.whPath_.toUri().toASCIIString()) ?  this.parent_.getDefaultTablePath(newName) : new Path(newLoc);

      try {
        // bugbug cannot move from one DFS to another I don't think - ask dhruba how to support this
        this.whPath_.getFileSystem(this.conf_).rename(this.whPath_, newPath);
      } catch(IOException e) {
        LOG.warn("got IOException in rename table: " + e.getMessage());
      }

      this.whPath_ = newPath;
      schema.setProperty(Constants.META_TABLE_LOCATION, newPath.toUri().toASCIIString());
      // for now no support for moving between dbs!
      // NOTE - bugbug, slight window when wrong schema on disk
      this.store_.rename(this.parent_, this.tableName_, this.parent_, newName);
      this.tableName_ = newName;
    } else if(newLoc.equals(this.schema_.getProperty(Constants.META_TABLE_LOCATION))) {
      // just location change
      Path newPath = new Path(newLoc);
      try {
        // bugbug cannot move from one DFS to another I don't think - ask dhruba how to support this
        this.whPath_.getFileSystem(this.conf_).rename(this.whPath_, newPath);
        this.whPath_ = newPath;
        schema.setProperty(Constants.META_TABLE_LOCATION, this.whPath_.toUri().toASCIIString());
      } catch(IOException e) {
        LOG.warn("got IOException in rename table: " + e.getMessage());
      }
    }
    this.schema_ = schema;
    save(true);
  }


  /**
   * create
   *
   *
   *
   *
   * @exception MetaException if any problems encountered during the creation
   *
   */
  static public Table create(DB parent, String tableName,  Properties schema, Configuration conf) throws MetaException {
    Table newTable  = new Table();
    newTable.parent_ = parent;
    newTable.tableName_ = tableName;
    newTable.conf_ = conf;
    newTable.o_rdonly_ = false;
    newTable.schema_ = schema;
    newTable.store_ = new FileStore(conf);

    if(MetaStoreUtils.validateName(tableName) == false) {
      throw new MetaException("Invalid table name: " + tableName + " - allowed characters are \\w and _") ;
    }

    String location = schema.getProperty(Constants.META_TABLE_LOCATION);

    if(location == null) {
      newTable.whPath_ = parent.getDefaultTablePath(tableName, (String)null);
      newTable.schema_.setProperty(Constants.META_TABLE_LOCATION, newTable.whPath_.toUri().toASCIIString());
    } else {
      newTable.whPath_ = new Path(location);
    }

    try {
      if (newTable.whPath_.getFileSystem(conf).exists(newTable.whPath_)) {
        // current unit tests will fail
        // throw new MetaException("for new table: " + tableName + " " + newTable.whPath_ + " already exists cannot create??");
      } else {
        newTable.whPath_.getFileSystem(conf).mkdirs(newTable.whPath_);
      }
    } catch(IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new MetaException(e.getMessage());
    }

    newTable.save(false);
    return newTable;
  }

  /**
   * save
   *
   * Save the schema. Note this will save the schema in the format of its choice, potentially doing some rewrites
   * from this code's version to a previous version for backwards compatability.
   *
   * @param overwrite - should this be a create or an alter basically
   * @exception MetaException if any problems saving the schema
   *
   */
  protected void save(boolean overwrite) throws MetaException {
    if (this.o_rdonly_) {
      throw new RuntimeException("cannot perform write operation on a read-only table");
    }
    // bugbug - should check for optomistic concurrency somewhere around here.

    this.store_.store(this.parent_, this.tableName_, this.schema_, overwrite);
    return ;
  }
}
