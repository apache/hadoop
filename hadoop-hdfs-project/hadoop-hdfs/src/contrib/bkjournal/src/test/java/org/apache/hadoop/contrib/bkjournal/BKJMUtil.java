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

import static org.junit.Assert.*;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.List;

import java.io.IOException;
import java.io.File;

/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down
 */
class BKJMUtil {
  protected static final Log LOG = LogFactory.getLog(BKJMUtil.class);

  int nextPort = 6000; // next port for additionally created bookies
  private Thread bkthread = null;
  private final static String zkEnsemble = "127.0.0.1:2181";
  int numBookies;

  BKJMUtil(final int numBookies) throws Exception {
    this.numBookies = numBookies;

    bkthread = new Thread() {
        public void run() {
          try {
            String[] args = new String[1];
            args[0] = String.valueOf(numBookies);
            LOG.info("Starting bk");
            LocalBookKeeper.main(args);
          } catch (InterruptedException e) {
            // go away quietly
          } catch (Exception e) {
            LOG.error("Error starting local bk", e);
          }
        }
      };
  }

  void start() throws Exception {
    bkthread.start();
    if (!LocalBookKeeper.waitForServerUp(zkEnsemble, 10000)) {
      throw new Exception("Error starting zookeeper/bookkeeper");
    }
    assertEquals("Not all bookies started",
                 numBookies, checkBookiesUp(numBookies, 10));
  }

  void teardown() throws Exception {
    if (bkthread != null) {
      bkthread.interrupt();
      bkthread.join();
    }
  }

  static ZooKeeper connectZooKeeper()
      throws IOException, KeeperException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    ZooKeeper zkc = new ZooKeeper(zkEnsemble, 3600, new Watcher() {
        public void process(WatchedEvent event) {
          if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            latch.countDown();
          }
        }
      });
    if (!latch.await(3, TimeUnit.SECONDS)) {
      throw new IOException("Zookeeper took too long to connect");
    }
    return zkc;
  }

  static URI createJournalURI(String path) throws Exception {
    return URI.create("bookkeeper://" + zkEnsemble + path);
  }

  static void addJournalManagerDefinition(Configuration conf) {
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + ".bookkeeper",
             "org.apache.hadoop.contrib.bkjournal.BookKeeperJournalManager");
  }

  BookieServer newBookie() throws Exception {
    int port = nextPort++;
    ServerConfiguration bookieConf = new ServerConfiguration();
    bookieConf.setBookiePort(port);
    File tmpdir = File.createTempFile("bookie" + Integer.toString(port) + "_",
                                      "test");
    tmpdir.delete();
    tmpdir.mkdir();

    bookieConf.setZkServers(zkEnsemble);
    bookieConf.setJournalDirName(tmpdir.getPath());
    bookieConf.setLedgerDirNames(new String[] { tmpdir.getPath() });

    BookieServer b = new BookieServer(bookieConf);
    b.start();
    for (int i = 0; i < 10 && !b.isRunning(); i++) {
      Thread.sleep(10000);
    }
    if (!b.isRunning()) {
      throw new IOException("Bookie would not start");
    }
    return b;
  }

  /**
   * Check that a number of bookies are available
   * @param count number of bookies required
   * @param timeout number of seconds to wait for bookies to start
   * @throws IOException if bookies are not started by the time the timeout hits
   */
  int checkBookiesUp(int count, int timeout) throws Exception {
    ZooKeeper zkc = connectZooKeeper();
    try {
      int mostRecentSize = 0;
      for (int i = 0; i < timeout; i++) {
        try {
          List<String> children = zkc.getChildren("/ledgers/available",
                                                  false);
          mostRecentSize = children.size();
          // Skip 'readonly znode' which is used for keeping R-O bookie details
          if (children.contains("readonly")) {
            mostRecentSize = children.size() - 1;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found " + mostRecentSize + " bookies up, "
                      + "waiting for " + count);
            if (LOG.isTraceEnabled()) {
              for (String child : children) {
                LOG.trace(" server: " + child);
              }
            }
          }
          if (mostRecentSize == count) {
            break;
          }
        } catch (KeeperException e) {
          // ignore
        }
        Thread.sleep(1000);
      }
      return mostRecentSize;
    } finally {
      zkc.close();
    }
  }
}
