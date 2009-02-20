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

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

public class MetaStoreTestBase extends TestCase {

  /**
   * Set things up - create mini dfs cluster and mount the fuse filesystem.
   */


  static final protected Configuration conf_;
  static final protected Path whRoot_;
  static final protected File msRoot_;
  static final protected FileSystem fileSys_;

  static {
    try {
      conf_ = new Configuration();
      File testRoot = File.createTempFile("hive.metastore.test",".dir");
      testRoot.delete();
      testRoot.mkdir();
      msRoot_ = new File(testRoot, "metadb");
      msRoot_.mkdir();
      msRoot_.deleteOnExit();
      File whRootFile = new File(testRoot,"hdfs");
      whRootFile.mkdir();
      whRoot_ = new Path(whRootFile.toString());
      conf_.set(HiveConf.ConfVars.METASTOREDIRECTORY.varname, msRoot_.getPath());
      conf_.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, whRootFile.toString());

      whRoot_.getFileSystem(conf_).mkdirs(whRoot_);
      fileSys_ = whRoot_.getFileSystem(conf_);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }

  }


  public MetaStoreTestBase() throws Exception {
  }

  public void createDB(String db) throws Exception {
    File f = new File(msRoot_, db + ".db");
    f.mkdir();
  }

  public void cleanup() throws Exception {
    MetaStoreUtils.recursiveDelete(msRoot_);
    msRoot_.mkdirs();
    fileSys_.delete(whRoot_, true);
    fileSys_.mkdirs(whRoot_);
  }

  static public Properties createSchema(String db, String name) throws Exception {
    Properties schema = new Properties();
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME,name);
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_DB, db);
    //schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_SERDE, org.apache.hadoop.hive.metastore.api.Constants.META_SERDE);
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS, "foo,bar");
    schema.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,"\t");
    //Path tPath = whRoot_.suffix("/" + db + ".db/" + name);
    //        schema.setProperty(Constants.META_TABLE_LOCATION, tPath.toString());
    return schema;
  }

  public void createTable(String db, String name) throws Exception {
    Properties schema = createSchema(db, name);
    fileSys_.mkdirs(new Path(schema.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION)));
    File f = new File(msRoot_, db + ".db/" + name);
    f.mkdir();
    f = new File(f, "schema");
    FileOutputStream fos = new FileOutputStream(f);
    schema.store(fos,"meta data");
    fos.close();
  }
}
