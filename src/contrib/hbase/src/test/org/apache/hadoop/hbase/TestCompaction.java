/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test compactions
 */
public class TestCompaction extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCompaction.class.getName());

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * Run compaction and flushing memcache
   * @throws Exception
   */
  public void testCompaction() throws Exception {
    HLog hlog = new HLog(this.localFs, this.testDir, this.conf);
    HTableDescriptor htd = createTableDescriptor(getName());
    HRegionInfo hri = new HRegionInfo(1, htd, null, null);
    final HRegion r =
      new HRegion(testDir, hlog, this.localFs, this.conf, hri, null);
    try {
      createStoreFile(r);
      assertFalse(r.needsCompaction());
      int compactionThreshold =
        this.conf.getInt("hbase.hstore.compactionThreshold", 3);
      for (int i = 0; i < compactionThreshold; i++) {
        createStoreFile(r);
      }
      assertTrue(r.needsCompaction());
      // Try to run compaction concurrent with a thread flush.
      addContent(new HRegionLoader(r), COLFAMILY_NAME1);
      Thread t1 = new Thread() {
        @Override
        public void run() {
          try {
            r.flushcache(false);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      Thread t2 = new Thread() {
        @Override
        public void run() {
          try {
            assertTrue(r.compactStores());
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      t1.setDaemon(true);
      t1.start();
      t2.setDaemon(true);
      t2.start();
      t1.join();
      t2.join();
    } finally {
      r.close();
      hlog.closeAndDelete();
    }
  }
  
  private void createStoreFile(final HRegion r) throws IOException {
    HRegionLoader loader = new HRegionLoader(r);
    for (int i = 0; i < 3; i++) {
      addContent(loader, COLFAMILY_NAME1);
    }
    r.flushcache(false);
  }
}