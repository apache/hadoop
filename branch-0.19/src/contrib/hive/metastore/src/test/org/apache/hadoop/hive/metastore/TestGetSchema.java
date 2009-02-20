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

import java.util.Properties;

import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestGetSchema extends MetaStoreTestBase {

  public TestGetSchema() throws Exception {
  }


  public void testGetSchema() throws Exception {
    try {
      String dbname = "bar";
      String name = "pete_ms_test2";
      DB db = DB.createDB(dbname, conf_);
      Properties schema = createSchema(dbname, name);
      Table t = Table.create(db, name, schema, conf_);
      assertTrue(t.equals(db.getTable(name, false)));
      assertTrue(db.getTable(name, false).getSchema().equals(schema));
      cleanup();
    } catch(MetaException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
