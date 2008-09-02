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

public class TestGetTables extends MetaStoreTestBase {

  public TestGetTables() throws Exception {
  }

  public void testGetTables() throws Exception {
    DB db = DB.createDB("foo1", conf_);
    {
      List<String> tables = db.getTables(".+");
      assertTrue(tables.size() == 0);
    }
    Table bar1 = Table.create(db, "bar1", createSchema("foo1","bar1"), conf_);
    Table bar2 = Table.create(db, "bar2", createSchema("foo1","bar2"), conf_);
    List<String> tables = db.getTables(".+");
    assertTrue(tables.size() == 2);
    assertTrue(tables.get(0).equals("bar1"));
    assertTrue(tables.get(1).equals("bar2"));
    cleanup();
  }
}
