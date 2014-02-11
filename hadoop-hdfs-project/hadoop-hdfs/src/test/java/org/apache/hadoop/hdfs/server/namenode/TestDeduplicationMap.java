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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext.DeduplicationMap;
import org.junit.Assert;
import org.junit.Test;

public class TestDeduplicationMap {
  @Test
  public void testDeduplicationMap() {
    DeduplicationMap<String> m = DeduplicationMap.newMap();
    Assert.assertEquals(1, m.getId("1"));
    Assert.assertEquals(2, m.getId("2"));
    Assert.assertEquals(3, m.getId("3"));
    Assert.assertEquals(1, m.getId("1"));
    Assert.assertEquals(2, m.getId("2"));
    Assert.assertEquals(3, m.getId("3"));
  }
}
