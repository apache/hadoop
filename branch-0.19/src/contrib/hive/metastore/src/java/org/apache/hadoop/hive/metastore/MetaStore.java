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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 *
 * MetaStore
 *
 * A MetaStore on top of HDFS. The goal is to expose Tables/Schemas to users rather than flat files and
 * Wiki that describes their structure and contents.
 * The MetaStore is used in conjunction with org.apache.hadoop.contrib.hive.contrib.serde.SerDe and flat files for
 * storing other schema information in addition to the SerDe library for each table.
 *
 *
 * The store has the concept of a db. The db is assumed to be optionally prefixed to the tableName and followed by a dot.
 * e.g., falcon.view_photo, default.tmp_pete.
 * In the schema store on disk, these dbs are stored as SCHEMA_STORE_PATH/<name>.db/ and then schemas under it are <tname>.dir/schema.
 * The schema store should have a symbolic link - ln -s SCHEMA_STORE_PATH SCHEMA_STORE_PATH/default.db .
 * All of a db's tables are stored in $ROOT_WAREHOUSE/dbname/tablename(s). The default db is special cased to
 * $ROOT_WAREHOUSE since dfs does not have symbolic links and since our data is already there.
 *
 * Internally, almost everywhere, the code ignores the db name - that is other than the following conditions:
 *
 * 1. When looking up the schema file from the table name (s/\./.db/)
 * 2. When deriving the table's intended location on DFS (s/^(.+?)\.(.+)$/$1/$2/)
 * 3. When calling getFields(db.table.field1.field2). Here it peels off the prefix and checks if it's a db name
 *
 *
 * TODOs:
 *
 * Think about making "db" an object and schema too.
 * Try to abstract away how we store dbs in the metastore and the warehouse. The latter is hard because the hive
 * runtime needs a way to lookup a table's schema from looking at the path to a specific partition that the map
 * is running on.
 *
 *
 *
 */
public class MetaStore {

  /**
   * Every schema must have a name, location and a serde
   */
  protected static final String [] RequiredSchemaKeys = {
    org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME,
    org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION,
    org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB,
  };

  public static final String LogKey = "hive.log";
  public static final String DefaultDB = "default";

  static public boolean dbExists(String dbName, Configuration conf) throws MetaException {
    return new FileStore(conf).dbExists(dbName);
  }

  static public List<String> getDbs(Configuration conf) throws MetaException {
    return new FileStore(conf).getDatabases();
  }
};
