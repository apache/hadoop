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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestTablePath extends MetaStoreTestBase {

  public TestTablePath() throws Exception {
  }


  public void testTablePath() throws Exception {
    try {
      String dbname = "default";
      String name = "pete_ms_test";
      DB db = new DB(dbname, conf_);
      // should be fully qualified
      Path res = db.getDefaultTablePath(name);
      Path reference = whRoot_.suffix((dbname.equals("default") ? "" : "/" + dbname + ".db") + "/" + name);
      assertTrue(res.equals(reference));
      cleanup();
    } catch(MetaException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
