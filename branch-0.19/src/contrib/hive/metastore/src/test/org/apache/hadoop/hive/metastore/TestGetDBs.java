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

import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestGetDBs extends MetaStoreTestBase {

  public TestGetDBs() throws Exception {
  }

  public void testGetDBs() throws Exception {
    try {
      {
        List<String> dbs = MetaStore.getDbs(conf_);
        assertTrue(dbs.size() == 1);
        assertTrue(dbs.get(0).equals("default"));
      }
      DB.createDB("foo1", conf_);
      DB.createDB("foo2", conf_);
      List<String> dbs = MetaStore.getDbs(conf_);
      assertTrue(dbs.size() == 3);
      assertTrue(dbs.get(0).equals("foo1"));
      assertTrue(dbs.get(1).equals("foo2"));
      assertTrue(dbs.get(2).equals("default"));
      cleanup();
    } catch(MetaException e) {
      e.printStackTrace();
      throw e;
    }
  }

}
