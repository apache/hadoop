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
package org.apache.hadoop.hbase.mapreduce;

import org.junit.Test;

import java.util.HashSet;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTableSplit {
  @Test
  public void testHashCode() {
    TableSplit split1 = new TableSplit("table".getBytes(), "row-start".getBytes(), "row-end".getBytes(), "location");
    TableSplit split2 = new TableSplit("table".getBytes(), "row-start".getBytes(), "row-end".getBytes(), "location");
    assertEquals (split1, split2);
    assertTrue   (split1.hashCode() == split2.hashCode());
    HashSet<TableSplit> set = new HashSet<TableSplit>(2);
    set.add(split1);
    set.add(split2);
    assertTrue(set.size() == 1);
  }
}
