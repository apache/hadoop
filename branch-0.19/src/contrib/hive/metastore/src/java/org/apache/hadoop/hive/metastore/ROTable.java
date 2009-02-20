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
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;


public class ROTable {

  protected String tableName_;
  protected Properties schema_;
  protected Configuration conf_;

  protected Path whPath_;
  protected DB parent_;
  //    protected FileSystem warehouse_;

  protected static final Log LOG = LogFactory.getLog(MetaStore.LogKey);
  protected FileStore store_;

  protected ROTable() { }


  public boolean equals(Table other) {
    return schema_.equals(other.schema_) &&
      tableName_.equals(other.tableName_) &&
      whPath_.equals(other.whPath_);
  }

  protected ROTable(DB parent, String tableName, Configuration conf) throws UnknownTableException, MetaException {
    parent_ = parent;
    tableName_ = tableName;
    conf_ = conf;
    store_ = new FileStore(conf); // only choice for now

    // check and load the schema
    if(store_.tableExists(parent_.getName(), tableName) == false) {
      throw new UnknownTableException("metadata does not exist");
    }
    schema_ = store_.load(parent, tableName);

    // check the table location is on the default dfs server for safety
    whPath_ = new Path(schema_.getProperty(Constants.META_TABLE_LOCATION));
    if(whPath_.toUri().relativize(parent.whRoot_.toUri()) == null) {
      // something potentially wrong as the stored warehouse not the same as our default
      // in general we want this, but in the short term, it can't happen
      LOG.warn(whPath_ + " is not the current default fs");
    }

    // check the data directory is there
    try {
      if(whPath_.getFileSystem(conf).exists(whPath_) == false) {
        throw new UnknownTableException("data does not exist:" + whPath_);
      }
    } catch(IOException e) {
      // ignore
    }
  }

  public Properties getSchema() {
    return schema_;
  }

  public Path getPath() {
    return whPath_;
  }

  /**
   * getPartitions
   *
   * Scan the file system and find all the partitions of this table
   * Not recursive right now - needs to be!
   *
   * @param the table name
   * @return a list of partitions - not full paths
   * @exception MetaException if gneneral problem or this table does not exist.
   */
  public ArrayList<String> getPartitions() throws MetaException {

    ArrayList<String> ret = new ArrayList<String> ();
    FileStatus[] dirs;

    try {
      dirs = whPath_.getFileSystem(conf_).listStatus(whPath_);
    } catch (IOException e) {
      throw new MetaException("DB Error: Table " + whPath_ + " missing?");
    }

    if(dirs == null) {
      throw new MetaException("FATAL: " + whPath_ + " does not seem to exist or maybe has no partitions in DFS");
    }


    Boolean equalsSeen = null;
    String partKey = null;

    for (int i = 0; i < dirs.length; i++) {
      String dname = dirs[i].getPath().getName();
      int sepidx = dname.indexOf('=');
      if (sepidx == -1) {
        if (equalsSeen != null && equalsSeen.booleanValue()) {
          throw new MetaException("DB Error: Table "+tableName_+" dir corrupted?");
        }
        equalsSeen = Boolean.valueOf(false);
        continue;
      } else {
        if (equalsSeen != null && !equalsSeen.booleanValue()) {
          throw new MetaException("DB Error: Table "+tableName_+" dir corrupted?");
        }
        equalsSeen = Boolean.valueOf(true);
      }
      String partVal = dname.substring(sepidx+1);
      if (partKey != null) {
        if (!partKey.equals(dname.substring(0,sepidx))) {
          throw new MetaException("DB Error: Directory "+dirs[i]);
        }
      } else {
        partKey = dname.substring(0,sepidx);
      }

      ret.add(partKey + "=" + partVal);
    }
    return ret;
  }
};

