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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ECSchemaManager;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestECSchemas {
  private MiniDFSCluster cluster;

  @Before
  public void before() throws IOException {
    cluster = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(0)
        .build();
    cluster.waitActive();
  }

  @After
  public void after() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetECSchemas() throws Exception {
    ECSchema[] ecSchemas = cluster.getFileSystem().getClient().getECSchemas();
    assertNotNull(ecSchemas);
    assertTrue("Should have at least one schema", ecSchemas.length > 0);
  }
}
