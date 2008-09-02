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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestAlter extends MetaStoreTestBase {

  public TestAlter() throws Exception {
  }


  public void testAlter() throws Exception {
    try {
      DB db = DB.createDB("foo1", conf_);
      {
        List<String> tables = db.getTables(".+");
        //        System.err.println("tables=" + tables);
        assertTrue(tables.size() == 0);
      }
      Table bar1 = Table.create(db, "bar1", createSchema("foo1","bar1"), conf_);
      {
        List<String> tables = db.getTables(".+");
        //        System.err.println("tables=" + tables);
        assertTrue(tables.size() == 1);
        assertTrue(tables.get(0).equals("bar1"));
      }
      Properties schema = bar1.getSchema();
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "bar2");
      bar1.alter(schema);
      {
        List<String> tables = db.getTables(".+");
        assertTrue(tables.size() == 1);
        //        System.err.println("tables=" + tables);
        assertTrue(tables.get(0).equals("bar2"));
        Path path = bar1.getPath();
        Path ref = new Path(db.getPath(), "bar2");
        //        System.err.println("path=" + path);
        assertTrue(path.equals(ref));
        FileStatus files[] = fileSys_.listStatus(db.getPath());
        //        for(FileStatus p: files) {
          //          System.err.println("path=" + p.getPath());
        //        }
        // bugbug not running below now because in file:// hdfs mode, seem to get two copies for each file
        // one with just /tmp/.../..  and another with file:/tmp/.../..
        //        assertTrue(files.length == 1);
        //        assertTrue(files[0].getPath().equals(ref));
      }
      //        cleanup();
    } catch(MetaException e) {
      e.printStackTrace();
      throw e;
    }
  }

}
