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

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.conf.Configuration;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BookKeeper Journal Manager
 *
 * To use, add the following to hdfs-site.xml.
 * <pre>
 * {@code
 * <property>
 *   <name>dfs.namenode.edits.dir</name>
 *   <value>bookkeeper://zk1:2181;zk2:2181;zk3:2181/hdfsjournal</value>
 * </property>
 *
 * <property>
 *   <name>dfs.namenode.edits.journalPlugin.bookkeeper</name>
 *   <value>org.apache.hadoop.contrib.bkjournal.BookKeeperJournalManager</value>
 * </property>
 * }
 * </pre>
 * The URI format for bookkeeper is bookkeeper://[zkEnsemble]/[rootZnode]
 * [zookkeeper ensemble] is a list of semi-colon separated, zookeeper host:port
 * pairs. In the example above there are 3 servers, in the ensemble,
 * zk1, zk2 &amp; zk3, each one listening on port 2181.
 *
 * [root znode] is the path of the zookeeper znode, under which the editlog
 * information will be stored.
 *
 * Other configuration options are:
 * <ul>
 *   <li><b>dfs.namenode.bookkeeperjournal.output-buffer-size</b>
 *       Number of bytes a bookkeeper journal stream will buffer before
 *       forcing a flush. Default is 1024.</li>
 *   <li><b>dfs.namenode.bookkeeperjournal.ensemble-size</b>
 *       Number of bookkeeper servers in edit log ledger ensembles. This
 *       is the number of bookkeeper servers which need to be available
 *       for the ledger to be writable. Default is 3.</li>
 *   <li><b>dfs.namenode.bookkeeperjournal.quorum-size</b>
 *       Number of bookkeeper servers in the write quorum. This is the
 *       number of bookkeeper servers which must have acknowledged the
 *       write of an entry before it is considered written.
 *       Default is 2.</li>
 *   <li><b>dfs.namenode.bookkeeperjournal.digestPw</b>
 *       Password to use when creating ledgers. </li>
 * </ul>
 */
public class BookKeeperJournalManager implements JournalManager {
  static final Log LOG = LogFactory.getLog(BookKeeperJournalManager.class);

  public static final String BKJM_OUTPUT_BUFFER_SIZE
    = "dfs.namenode.bookkeeperjournal.output-buffer-size";
  public static final int BKJM_OUTPUT_BUFFER_SIZE_DEFAULT = 1024;

  public static final String BKJM_BOOKKEEPER_ENSEMBLE_SIZE
    = "dfs.namenode.bookkeeperjournal.ensemble-size";
  public static final int BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT = 3;

 public static final String BKJM_BOOKKEEPER_QUORUM_SIZE
    = "dfs.namenode.bookkeeperjournal.quorum-size";
  public static final int BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT = 2;

  public static final String BKJM_BOOKKEEPER_DIGEST_PW
    = "dfs.namenode.bookkeeperjournal.digestPw";
  public static final String BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT = "";

  private static final int BKJM_LAYOUT_VERSION = -1;

  private final ZooKeeper zkc;
  private final Configuration conf;
  private final BookKeeper bkc;
  private final WriteLock wl;
  private final String ledgerPath;
  private final MaxTxId maxTxId;
  private final int ensembleSize;
  private final int quorumSize;
  private final String digestpw;
  private final CountDownLatch zkConnectLatch;

  private LedgerHandle currentLedger = null;

  private int bytesToInt(byte[] b) {
    assert b.length >= 4;
    return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
  }

  private byte[] intToBytes(int i) {
    return new byte[] {
      (byte)(i >> 24),
      (byte)(i >> 16),
      (byte)(i >> 8),
      (byte)(i) };
  }

  /**
   * Construct a Bookkeeper journal manager.
   */
  public BookKeeperJournalManager(Configuration conf, URI uri)
      throws IOException {
    this.conf = conf;
    String zkConnect = uri.getAuthority().replace(";", ",");
    String zkPath = uri.getPath();
    ensembleSize = conf.getInt(BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                               BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT);
    quorumSize = conf.getInt(BKJM_BOOKKEEPER_QUORUM_SIZE,
                             BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT);

    ledgerPath = zkPath + "/ledgers";
    String maxTxIdPath = zkPath + "/maxtxid";
    String lockPath = zkPath + "/lock";
    String versionPath = zkPath + "/version";
    digestpw = conf.get(BKJM_BOOKKEEPER_DIGEST_PW,
                        BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT);

    try {
      zkConnectLatch = new CountDownLatch(1);
      zkc = new ZooKeeper(zkConnect, 3000, new ZkConnectionWatcher());
      if (!zkConnectLatch.await(6000, TimeUnit.MILLISECONDS)) {
        throw new IOException("Error connecting to zookeeper");
      }
      if (zkc.exists(zkPath, false) == null) {
        zkc.create(zkPath, new byte[] {'0'},
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      Stat versionStat = zkc.exists(versionPath, false);
      if (versionStat != null) {
        byte[] d = zkc.getData(versionPath, false, versionStat);
        // There's only one version at the moment
        assert bytesToInt(d) == BKJM_LAYOUT_VERSION;
      } else {
        zkc.create(versionPath, intToBytes(BKJM_LAYOUT_VERSION),
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      if (zkc.exists(ledgerPath, false) == null) {
        zkc.create(ledgerPath, new byte[] {'0'},
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      bkc = new BookKeeper(new ClientConfiguration(),
                           zkc);
    } catch (Exception e) {
      throw new IOException("Error initializing zk", e);
    }

    wl = new WriteLock(zkc, lockPath);
    maxTxId = new MaxTxId(zkc, maxTxIdPath);
  }

  /**
   * Start a new log segment in a BookKeeper ledger.
   * First ensure that we have the write lock for this journal.
   * Then create a ledger and stream based on that ledger.
   * The ledger id is written to the inprogress znode, so that in the
   * case of a crash, a recovery process can find the ledger we were writing
   * to when we crashed.
   * @param txId First transaction id to be written to the stream
   */
  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    wl.acquire();

    if (txId <= maxTxId.get()) {
      throw new IOException("We've already seen " + txId
          + ". A new stream cannot be created with it");
    }
    if (currentLedger != null) {
      throw new IOException("Already writing to a ledger, id="
                            + currentLedger.getId());
    }
    try {
      currentLedger = bkc.createLedger(ensembleSize, quorumSize,
                                       BookKeeper.DigestType.MAC,
                                       digestpw.getBytes());
      String znodePath = inprogressZNode();
      EditLogLedgerMetadata l = new EditLogLedgerMetadata(znodePath,
          HdfsConstants.LAYOUT_VERSION,  currentLedger.getId(), txId);
      /* Write the ledger metadata out to the inprogress ledger znode
       * This can fail if for some reason our write lock has
       * expired (@see WriteLock) and another process has managed to
       * create the inprogress znode.
       * In this case, throw an exception. We don't want to continue
       * as this would lead to a split brain situation.
       */
      l.write(zkc, znodePath);

      return new BookKeeperEditLogOutputStream(conf, currentLedger, wl);
    } catch (Exception e) {
      if (currentLedger != null) {
        try {
          currentLedger.close();
        } catch (Exception e2) {
          //log & ignore, an IOException will be thrown soon
          LOG.error("Error closing ledger", e2);
        }
      }
      throw new IOException("Error creating ledger", e);
    }
  }

  /**
   * Finalize a log segment. If the journal manager is currently
   * writing to a ledger, ensure that this is the ledger of the log segment
   * being finalized.
   *
   * Otherwise this is the recovery case. In the recovery case, ensure that
   * the firstTxId of the ledger matches firstTxId for the segment we are
   * trying to finalize.
   */
  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    String inprogressPath = inprogressZNode();
    try {
      Stat inprogressStat = zkc.exists(inprogressPath, false);
      if (inprogressStat == null) {
        throw new IOException("Inprogress znode " + inprogressPath
                              + " doesn't exist");
      }

      wl.checkWriteLock();
      EditLogLedgerMetadata l
        =  EditLogLedgerMetadata.read(zkc, inprogressPath);

      if (currentLedger != null) { // normal, non-recovery case
        if (l.getLedgerId() == currentLedger.getId()) {
          try {
            currentLedger.close();
          } catch (BKException bke) {
            LOG.error("Error closing current ledger", bke);
          }
          currentLedger = null;
        } else {
          throw new IOException(
              "Active ledger has different ID to inprogress. "
              + l.getLedgerId() + " found, "
              + currentLedger.getId() + " expected");
        }
      }

      if (l.getFirstTxId() != firstTxId) {
        throw new IOException("Transaction id not as expected, "
            + l.getFirstTxId() + " found, " + firstTxId + " expected");
      }

      l.finalizeLedger(lastTxId);
      String finalisedPath = finalizedLedgerZNode(firstTxId, lastTxId);
      try {
        l.write(zkc, finalisedPath);
      } catch (KeeperException.NodeExistsException nee) {
        if (!l.verify(zkc, finalisedPath)) {
          throw new IOException("Node " + finalisedPath + " already exists"
                                + " but data doesn't match");
        }
      }
      maxTxId.store(lastTxId);
      zkc.delete(inprogressPath, inprogressStat.getVersion());
    } catch (KeeperException e) {
      throw new IOException("Error finalising ledger", e);
    } catch (InterruptedException ie) {
      throw new IOException("Error finalising ledger", ie);
    } finally {
      wl.release();
    }
  }

  // TODO(HA): Handle inProgressOk
  @Override
  public EditLogInputStream getInputStream(long fromTxnId, boolean inProgressOk)
      throws IOException {
    for (EditLogLedgerMetadata l : getLedgerList()) {
      if (l.getFirstTxId() == fromTxnId) {
        try {
          LedgerHandle h = bkc.openLedger(l.getLedgerId(),
                                          BookKeeper.DigestType.MAC,
                                          digestpw.getBytes());
          return new BookKeeperEditLogInputStream(h, l);
        } catch (Exception e) {
          throw new IOException("Could not open ledger for " + fromTxnId, e);
        }
      }
    }
    throw new IOException("No ledger for fromTxnId " + fromTxnId + " found.");
  }

  // TODO(HA): Handle inProgressOk
  @Override
  public long getNumberOfTransactions(long fromTxnId, boolean inProgressOk)
      throws IOException {
    long count = 0;
    long expectedStart = 0;
    for (EditLogLedgerMetadata l : getLedgerList()) {
      if (l.isInProgress()) {
        long endTxId = recoverLastTxId(l);
        if (endTxId == HdfsConstants.INVALID_TXID) {
          break;
        }
        count += (endTxId - l.getFirstTxId()) + 1;
        break;
      }

      if (l.getFirstTxId() < fromTxnId) {
        continue;
      } else if (l.getFirstTxId() == fromTxnId) {
        count = (l.getLastTxId() - l.getFirstTxId()) + 1;
        expectedStart = l.getLastTxId() + 1;
      } else {
        if (expectedStart != l.getFirstTxId()) {
          if (count == 0) {
            throw new CorruptionException("StartTxId " + l.getFirstTxId()
                + " is not as expected " + expectedStart
                + ". Gap in transaction log?");
          } else {
            break;
          }
        }
        count += (l.getLastTxId() - l.getFirstTxId()) + 1;
        expectedStart = l.getLastTxId() + 1;
      }
    }
    return count;
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    wl.acquire();

    synchronized (this) {
      try {
        EditLogLedgerMetadata l
          = EditLogLedgerMetadata.read(zkc, inprogressZNode());
        long endTxId = recoverLastTxId(l);
        if (endTxId == HdfsConstants.INVALID_TXID) {
          LOG.error("Unrecoverable corruption has occurred in segment "
                    + l.toString() + " at path " + inprogressZNode()
                    + ". Unable to continue recovery.");
          throw new IOException("Unrecoverable corruption, please check logs.");
        }
        finalizeLogSegment(l.getFirstTxId(), endTxId);
      } catch (KeeperException.NoNodeException nne) {
          // nothing to recover, ignore
      } finally {
        if (wl.haveLock()) {
          wl.release();
        }
      }
    }
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    for (EditLogLedgerMetadata l : getLedgerList()) {
      if (!l.isInProgress()
          && l.getLastTxId() < minTxIdToKeep) {
        try {
          Stat stat = zkc.exists(l.getZkPath(), false);
          zkc.delete(l.getZkPath(), stat.getVersion());
          bkc.deleteLedger(l.getLedgerId());
        } catch (InterruptedException ie) {
          LOG.error("Interrupted while purging " + l, ie);
        } catch (BKException bke) {
          LOG.error("Couldn't delete ledger from bookkeeper", bke);
        } catch (KeeperException ke) {
          LOG.error("Error deleting ledger entry in zookeeper", ke);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      bkc.close();
      zkc.close();
    } catch (Exception e) {
      throw new IOException("Couldn't close zookeeper client", e);
    }
  }

  /**
   * Set the amount of memory that this stream should use to buffer edits.
   * Setting this will only affect future output stream. Streams
   * which have currently be created won't be affected.
   */
  @Override
  public void setOutputBufferCapacity(int size) {
    conf.getInt(BKJM_OUTPUT_BUFFER_SIZE, size);
  }

  /**
   * Find the id of the last edit log transaction writen to a edit log
   * ledger.
   */
  private long recoverLastTxId(EditLogLedgerMetadata l) throws IOException {
    try {
      LedgerHandle lh = bkc.openLedger(l.getLedgerId(),
                                       BookKeeper.DigestType.MAC,
                                       digestpw.getBytes());
      long lastAddConfirmed = lh.getLastAddConfirmed();
      BookKeeperEditLogInputStream in
        = new BookKeeperEditLogInputStream(lh, l, lastAddConfirmed);

      long endTxId = HdfsConstants.INVALID_TXID;
      FSEditLogOp op = in.readOp();
      while (op != null) {
        if (endTxId == HdfsConstants.INVALID_TXID
            || op.getTransactionId() == endTxId+1) {
          endTxId = op.getTransactionId();
        }
        op = in.readOp();
      }
      return endTxId;
    } catch (Exception e) {
      throw new IOException("Exception retreiving last tx id for ledger " + l,
                            e);
    }
  }

  /**
   * Get a list of all segments in the journal.
   */
  private List<EditLogLedgerMetadata> getLedgerList() throws IOException {
    List<EditLogLedgerMetadata> ledgers
      = new ArrayList<EditLogLedgerMetadata>();
    try {
      List<String> ledgerNames = zkc.getChildren(ledgerPath, false);
      for (String n : ledgerNames) {
        ledgers.add(EditLogLedgerMetadata.read(zkc, ledgerPath + "/" + n));
      }
    } catch (Exception e) {
      throw new IOException("Exception reading ledger list from zk", e);
    }

    Collections.sort(ledgers, EditLogLedgerMetadata.COMPARATOR);
    return ledgers;
  }

  /**
   * Get the znode path for a finalize ledger
   */
  String finalizedLedgerZNode(long startTxId, long endTxId) {
    return String.format("%s/edits_%018d_%018d",
                         ledgerPath, startTxId, endTxId);
  }

  /**
   * Get the znode path for the inprogressZNode
   */
  String inprogressZNode() {
    return ledgerPath + "/inprogress";
  }

  /**
   * Simple watcher to notify when zookeeper has connected
   */
  private class ZkConnectionWatcher implements Watcher {
    public void process(WatchedEvent event) {
      if (Event.KeeperState.SyncConnected.equals(event.getState())) {
        zkConnectLatch.countDown();
      }
    }
  }
}
