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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestPartitions extends MetaStoreTestBase {

  public TestPartitions() throws Exception {
  }



  public void testPartitions() throws Exception {
    try {
      DB db = DB.createDB("foo1", conf_);
      Table bar1 = Table.create(db, "bar1", createSchema("foo1","bar1"), conf_);
      {
        List<String> partitions = bar1.getPartitions();
        assertTrue(partitions.size() == 0);
      }
      Path part1 = new Path(bar1.getPath(),"ds=2008-01-01");
      Path part2 = new Path(bar1.getPath(),"ds=2008-01-02");
      fileSys_.mkdirs(part1);
      fileSys_.mkdirs(part2);
      List<String> partitions = bar1.getPartitions();
      assertTrue(partitions.size() == 2);
      assertTrue(partitions.get(0).equals("ds=2008-01-01"));
      assertTrue(partitions.get(1).equals("ds=2008-01-02"));
      cleanup();
    } catch(MetaException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
