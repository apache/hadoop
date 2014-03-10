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
package org.apache.hadoop.contrib.bkjournal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for the bkjm's streams
 */
public class TestBookKeeperEditLogStreams {
  static final Log LOG = LogFactory.getLog(TestBookKeeperEditLogStreams.class);

  private static BKJMUtil bkutil;
  private final static int numBookies = 3;

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkutil = new BKJMUtil(numBookies);
    bkutil.start();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    bkutil.teardown();
  }

  /**
   * Test that bkjm will refuse open a stream on an empty
   * ledger.
   */
  @Test
  public void testEmptyInputStream() throws Exception {
    ZooKeeper zk = BKJMUtil.connectZooKeeper();

    BookKeeper bkc = new BookKeeper(new ClientConfiguration(), zk);
    try {
      LedgerHandle lh = bkc.createLedger(BookKeeper.DigestType.CRC32, "foobar"
          .getBytes());
      lh.close();

      EditLogLedgerMetadata metadata = new EditLogLedgerMetadata("/foobar",
          HdfsConstants.NAMENODE_LAYOUT_VERSION, lh.getId(), 0x1234);
      try {
        new BookKeeperEditLogInputStream(lh, metadata, -1);
        fail("Shouldn't get this far, should have thrown");
      } catch (IOException ioe) {
        assertTrue(ioe.getMessage().contains("Invalid first bk entry to read"));
      }

      metadata = new EditLogLedgerMetadata("/foobar",
          HdfsConstants.NAMENODE_LAYOUT_VERSION, lh.getId(), 0x1234);
      try {
        new BookKeeperEditLogInputStream(lh, metadata, 0);
        fail("Shouldn't get this far, should have thrown");
      } catch (IOException ioe) {
        assertTrue(ioe.getMessage().contains("Invalid first bk entry to read"));
      }
    } finally {
      bkc.close();
      zk.close();
    }
  }
}
