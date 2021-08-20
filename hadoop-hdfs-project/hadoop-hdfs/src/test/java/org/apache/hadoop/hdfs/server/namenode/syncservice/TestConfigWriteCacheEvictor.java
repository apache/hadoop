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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the effect of configuring WriteCacheEvictor.
 */
public class TestConfigWriteCacheEvictor {

  @Test
  public void testConfig() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
        SimpleWriteCacheEvictor.class.getName());
    WriteCacheEvictor evictor = WriteCacheEvictor.getInstance(conf, null);
    Assert.assertTrue(evictor.getClass() == SimpleWriteCacheEvictor.class);
    WriteCacheEvictor.destroyInstance();
    conf.set(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
        FIFOWriteCacheEvictor.class.getName());
    evictor = WriteCacheEvictor.getInstance(conf, null);
    Assert.assertTrue(evictor.getClass() == FIFOWriteCacheEvictor.class);

    // Test default configuration.
    conf = new Configuration();
    WriteCacheEvictor.destroyInstance();
    evictor = WriteCacheEvictor.getInstance(conf, null);
    Assert.assertTrue(evictor.getClass() == FIFOWriteCacheEvictor.class);
    Assert.assertTrue(evictor.getWriteCacheQuota() ==
        DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_QUOTA_SIZE_DEFAULT);
  }
}
