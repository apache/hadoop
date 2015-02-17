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
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.conf.Configuration;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.ZkUtils;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.ZKUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;

import java.net.URI;

import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.contrib.bkjournal.BKJournalProtos.VersionProto;
import com.google.protobuf.TextFormat;
import static com.google.common.base.Charsets.UTF_8;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.common.annotations.VisibleForTesting;
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
 *   <name>dfs.namenode.edits.journal-plugin.bookkeeper</name>
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
 *   <li><b>dfs.namenode.bookkeeperjournal.zk.session.timeout</b>
 *       Session timeout for Zookeeper client from BookKeeper Journal Manager.
 *       Hadoop recommends that, this value should be less than the ZKFC 
 *       session timeout value. Default value is 3000.</li>
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
  
  public static final String BKJM_ZK_SESSION_TIMEOUT 
    = "dfs.namenode.bookkeeperjournal.zk.session.timeout";
  public static final int BKJM_ZK_SESSION_TIMEOUT_DEFAULT = 3000;

  private static final String BKJM_EDIT_INPROGRESS = "inprogress_";

  public static final String BKJM_ZK_LEDGERS_AVAILABLE_PATH
    = "dfs.namenode.bookkeeperjournal.zk.availablebookies";

  public static final String BKJM_ZK_LEDGERS_AVAILABLE_PATH_DEFAULT
    = "/ledgers/available";

  public static final String BKJM_BOOKKEEPER_SPECULATIVE_READ_TIMEOUT_MS
    = "dfs.namenode.bookkeeperjournal.speculativeReadTimeoutMs";
  public static final int BKJM_BOOKKEEPER_SPECULATIVE_READ_TIMEOUT_DEFAULT
    = 2000;

  public static final String BKJM_BOOKKEEPER_READ_ENTRY_TIMEOUT_SEC
    = "dfs.namenode.bookkeeperjournal.readEntryTimeoutSec";
  public static final int BKJM_BOOKKEEPER_READ_ENTRY_TIMEOUT_DEFAULT = 5;

  public static final String BKJM_BOOKKEEPER_ACK_QUORUM_SIZE 
    = "dfs.namenode.bookkeeperjournal.ack.quorum-size";

  public static final String BKJM_BOOKKEEPER_ADD_ENTRY_TIMEOUT_SEC
    = "dfs.namenode.bookkeeperjournal.addEntryTimeoutSec";
  public static final int BKJM_BOOKKEEPER_ADD_ENTRY_TIMEOUT_DEFAULT = 5;

  private ZooKeeper zkc;
  private final Configuration conf;
  private final BookKeeper bkc;
  private final CurrentInprogress ci;
  private final String basePath;
  private final String ledgerPath;
  private final String versionPath;
  private final MaxTxId maxTxId;
  private final int ensembleSize;
  private final int quorumSize;
  private final int ackQuorumSize;
  private final int addEntryTimeout;
  private final String digestpw;
  private final int speculativeReadTimeout;
  private final int readEntryTimeout;
  private final CountDownLatch zkConnectLatch;
  private final NamespaceInfo nsInfo;
  private boolean initialized = false;
  private LedgerHandle currentLedger = null;

  /**
   * Construct a Bookkeeper journal manager.
   */
  public BookKeeperJournalManager(Configuration conf, URI uri,
      NamespaceInfo nsInfo) throws IOException {
    this.conf = conf;
    this.nsInfo = nsInfo;

    String zkConnect = uri.getAuthority().replace(";", ",");
    basePath = uri.getPath();
    ensembleSize = conf.getInt(BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                               BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT);
    quorumSize = conf.getInt(BKJM_BOOKKEEPER_QUORUM_SIZE,
                             BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT);
    ackQuorumSize = conf.getInt(BKJM_BOOKKEEPER_ACK_QUORUM_SIZE, quorumSize);
    addEntryTimeout = conf.getInt(BKJM_BOOKKEEPER_ADD_ENTRY_TIMEOUT_SEC,
                             BKJM_BOOKKEEPER_ADD_ENTRY_TIMEOUT_DEFAULT);
    speculativeReadTimeout = conf.getInt(
                             BKJM_BOOKKEEPER_SPECULATIVE_READ_TIMEOUT_MS,
                             BKJM_BOOKKEEPER_SPECULATIVE_READ_TIMEOUT_DEFAULT);
    readEntryTimeout = conf.getInt(BKJM_BOOKKEEPER_READ_ENTRY_TIMEOUT_SEC,
                             BKJM_BOOKKEEPER_READ_ENTRY_TIMEOUT_DEFAULT);

    ledgerPath = basePath + "/ledgers";
    String maxTxIdPath = basePath + "/maxtxid";
    String currentInprogressNodePath = basePath + "/CurrentInprogress";
    versionPath = basePath + "/version";
    digestpw = conf.get(BKJM_BOOKKEEPER_DIGEST_PW,
                        BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT);

    try {
      zkConnectLatch = new CountDownLatch(1);
      int bkjmZKSessionTimeout = conf.getInt(BKJM_ZK_SESSION_TIMEOUT,
          BKJM_ZK_SESSION_TIMEOUT_DEFAULT);
      zkc = new ZooKeeper(zkConnect, bkjmZKSessionTimeout,
          new ZkConnectionWatcher());
      // Configured zk session timeout + some extra grace period (here
      // BKJM_ZK_SESSION_TIMEOUT_DEFAULT used as grace period)
      int zkConnectionLatchTimeout = bkjmZKSessionTimeout
          + BKJM_ZK_SESSION_TIMEOUT_DEFAULT;
      if (!zkConnectLatch
          .await(zkConnectionLatchTimeout, TimeUnit.MILLISECONDS)) {
        throw new IOException("Error connecting to zookeeper");
      }

      prepareBookKeeperEnv();
      ClientConfiguration clientConf = new ClientConfiguration();
      clientConf.setSpeculativeReadTimeout(speculativeReadTimeout);
      clientConf.setReadEntryTimeout(readEntryTimeout);
      clientConf.setAddEntryTimeout(addEntryTimeout);
      bkc = new BookKeeper(clientConf, zkc);
    } catch (KeeperException e) {
      throw new IOException("Error initializing zk", e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while initializing bk journal manager",
                            ie);
    }

    ci = new CurrentInprogress(zkc, currentInprogressNodePath);
    maxTxId = new MaxTxId(zkc, maxTxIdPath);
  }

  /**
   * Pre-creating bookkeeper metadata path in zookeeper.
   */
  private void prepareBookKeeperEnv() throws IOException {
    // create bookie available path in zookeeper if it doesn't exists
    final String zkAvailablePath = conf.get(BKJM_ZK_LEDGERS_AVAILABLE_PATH,
        BKJM_ZK_LEDGERS_AVAILABLE_PATH_DEFAULT);
    final CountDownLatch zkPathLatch = new CountDownLatch(1);

    final AtomicBoolean success = new AtomicBoolean(false);
    StringCallback callback = new StringCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, String name) {
        if (KeeperException.Code.OK.intValue() == rc
            || KeeperException.Code.NODEEXISTS.intValue() == rc) {
          LOG.info("Successfully created bookie available path : "
              + zkAvailablePath);
          success.set(true);
        } else {
          KeeperException.Code code = KeeperException.Code.get(rc);
          LOG.error("Error : "
                  + KeeperException.create(code, path).getMessage()
                  + ", failed to create bookie available path : "
                  + zkAvailablePath);
        }
        zkPathLatch.countDown();
      }
    };
    ZkUtils.asyncCreateFullPathOptimistic(zkc, zkAvailablePath, new byte[0],
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, callback, null);

    try {
      if (!zkPathLatch.await(zkc.getSessionTimeout(), TimeUnit.MILLISECONDS)
          || !success.get()) {
        throw new IOException("Couldn't create bookie available path :"
            + zkAvailablePath + ", timed out " + zkc.getSessionTimeout()
            + " millis");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          "Interrupted when creating the bookie available path : "
              + zkAvailablePath, e);
    }
  }

  @Override
  public void format(NamespaceInfo ns) throws IOException {
    try {
      // delete old info
      Stat baseStat = null;
      Stat ledgerStat = null;
      if ((baseStat = zkc.exists(basePath, false)) != null) {
        if ((ledgerStat = zkc.exists(ledgerPath, false)) != null) {
          for (EditLogLedgerMetadata l : getLedgerList(true)) {
            try {
              bkc.deleteLedger(l.getLedgerId());
            } catch (BKException.BKNoSuchLedgerExistsException bke) {
              LOG.warn("Ledger " + l.getLedgerId() + " does not exist;"
                       + " Cannot delete.");
            }
          }
        }
        ZKUtil.deleteRecursive(zkc, basePath);
      }

      // should be clean now.
      zkc.create(basePath, new byte[] {'0'},
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      VersionProto.Builder builder = VersionProto.newBuilder();
      builder.setNamespaceInfo(PBHelper.convert(ns))
        .setLayoutVersion(BKJM_LAYOUT_VERSION);

      byte[] data = TextFormat.printToString(builder.build()).getBytes(UTF_8);
      zkc.create(versionPath, data,
                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      zkc.create(ledgerPath, new byte[] {'0'},
                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException ke) {
      LOG.error("Error accessing zookeeper to format", ke);
      throw new IOException("Error accessing zookeeper to format", ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during format", ie);
    } catch (BKException bke) {
      throw new IOException("Error cleaning up ledgers during format", bke);
    }
  }
  
  @Override
  public boolean hasSomeData() throws IOException {
    try {
      return zkc.exists(basePath, false) != null;
    } catch (KeeperException ke) {
      throw new IOException("Couldn't contact zookeeper", ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while checking for data", ie);
    }
  }

  synchronized private void checkEnv() throws IOException {
    if (!initialized) {
      try {
        Stat versionStat = zkc.exists(versionPath, false);
        if (versionStat == null) {
          throw new IOException("Environment not initialized. "
                                +"Have you forgotten to format?");
        }
        byte[] d = zkc.getData(versionPath, false, versionStat);

        VersionProto.Builder builder = VersionProto.newBuilder();
        TextFormat.merge(new String(d, UTF_8), builder);
        if (!builder.isInitialized()) {
          throw new IOException("Invalid/Incomplete data in znode");
        }
        VersionProto vp = builder.build();

        // There's only one version at the moment
        assert vp.getLayoutVersion() == BKJM_LAYOUT_VERSION;

        NamespaceInfo readns = PBHelper.convert(vp.getNamespaceInfo());

        if (nsInfo.getNamespaceID() != readns.getNamespaceID() ||
            !nsInfo.clusterID.equals(readns.getClusterID()) ||
            !nsInfo.getBlockPoolID().equals(readns.getBlockPoolID())) {
          String err = String.format("Environment mismatch. Running process %s"
                                     +", stored in ZK %s", nsInfo, readns);
          LOG.error(err);
          throw new IOException(err);
        }

        ci.init();
        initialized = true;
      } catch (KeeperException ke) {
        throw new IOException("Cannot access ZooKeeper", ke);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while checking environment", ie);
      }
    }
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
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    checkEnv();

    if (txId <= maxTxId.get()) {
      throw new IOException("We've already seen " + txId
          + ". A new stream cannot be created with it");
    }

    try {
      String existingInprogressNode = ci.read();
      if (null != existingInprogressNode
          && zkc.exists(existingInprogressNode, false) != null) {
        throw new IOException("Inprogress node already exists");
      }
      if (currentLedger != null) {
        // bookkeeper errored on last stream, clean up ledger
        currentLedger.close();
      }
      currentLedger = bkc.createLedger(ensembleSize, quorumSize, ackQuorumSize,
                                       BookKeeper.DigestType.MAC,
                                       digestpw.getBytes(Charsets.UTF_8));
    } catch (BKException bke) {
      throw new IOException("Error creating ledger", bke);
    } catch (KeeperException ke) {
      throw new IOException("Error in zookeeper while creating ledger", ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted creating ledger", ie);
    }

    try {
      String znodePath = inprogressZNode(txId);
      EditLogLedgerMetadata l = new EditLogLedgerMetadata(znodePath,
          layoutVersion, currentLedger.getId(), txId);
      /* Write the ledger metadata out to the inprogress ledger znode
       * This can fail if for some reason our write lock has
       * expired (@see WriteLock) and another process has managed to
       * create the inprogress znode.
       * In this case, throw an exception. We don't want to continue
       * as this would lead to a split brain situation.
       */
      l.write(zkc, znodePath);

      maxTxId.store(txId);
      ci.update(znodePath);
      return new BookKeeperEditLogOutputStream(conf, currentLedger);
    } catch (KeeperException ke) {
      cleanupLedger(currentLedger);
      throw new IOException("Error storing ledger metadata", ke);
    }
  }

  private void cleanupLedger(LedgerHandle lh) {
    try {
      long id = currentLedger.getId();
      currentLedger.close();
      bkc.deleteLedger(id);
    } catch (BKException bke) {
      //log & ignore, an IOException will be thrown soon
      LOG.error("Error closing ledger", bke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while closing ledger", ie);
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
    checkEnv();

    String inprogressPath = inprogressZNode(firstTxId);
    try {
      Stat inprogressStat = zkc.exists(inprogressPath, false);
      if (inprogressStat == null) {
        throw new IOException("Inprogress znode " + inprogressPath
                              + " doesn't exist");
      }

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
      String inprogressPathFromCI = ci.read();
      if (inprogressPath.equals(inprogressPathFromCI)) {
        ci.clear();
      }
    } catch (KeeperException e) {
      throw new IOException("Error finalising ledger", e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Error finalising ledger", ie);
    } 
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk)
      throws IOException {
    List<EditLogLedgerMetadata> currentLedgerList = getLedgerList(fromTxId,
        inProgressOk);
    try {
      BookKeeperEditLogInputStream elis = null;
      for (EditLogLedgerMetadata l : currentLedgerList) {
        long lastTxId = l.getLastTxId();
        if (l.isInProgress()) {
          lastTxId = recoverLastTxId(l, false);
        }
        // Check once again, required in case of InProgress and is case of any
        // gap.
        if (fromTxId >= l.getFirstTxId() && fromTxId <= lastTxId) {
          LedgerHandle h;
          if (l.isInProgress()) { // we don't want to fence the current journal
            h = bkc.openLedgerNoRecovery(l.getLedgerId(),
                BookKeeper.DigestType.MAC, digestpw.getBytes(Charsets.UTF_8));
          } else {
            h = bkc.openLedger(l.getLedgerId(), BookKeeper.DigestType.MAC,
                digestpw.getBytes(Charsets.UTF_8));
          }
          elis = new BookKeeperEditLogInputStream(h, l);
          elis.skipTo(fromTxId);
        } else {
          // If mismatches then there might be some gap, so we should not check
          // further.
          return;
        }
        streams.add(elis);
        if (elis.getLastTxId() == HdfsConstants.INVALID_TXID) {
          return;
        }
        fromTxId = elis.getLastTxId() + 1;
      }
    } catch (BKException e) {
      throw new IOException("Could not open ledger for " + fromTxId, e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted opening ledger for " + fromTxId, ie);
    }
  }

  long getNumberOfTransactions(long fromTxId, boolean inProgressOk)
      throws IOException {
    long count = 0;
    long expectedStart = 0;
    for (EditLogLedgerMetadata l : getLedgerList(inProgressOk)) {
      long lastTxId = l.getLastTxId();
      if (l.isInProgress()) {
        lastTxId = recoverLastTxId(l, false);
        if (lastTxId == HdfsConstants.INVALID_TXID) {
          break;
        }
      }

      assert lastTxId >= l.getFirstTxId();

      if (lastTxId < fromTxId) {
        continue;
      } else if (l.getFirstTxId() <= fromTxId && lastTxId >= fromTxId) {
        // we can start in the middle of a segment
        count = (lastTxId - l.getFirstTxId()) + 1;
        expectedStart = lastTxId + 1;
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
        count += (lastTxId - l.getFirstTxId()) + 1;
        expectedStart = lastTxId + 1;
      }
    }
    return count;
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    checkEnv();

    synchronized (this) {
      try {
        List<String> children = zkc.getChildren(ledgerPath, false);
        for (String child : children) {
          if (!child.startsWith(BKJM_EDIT_INPROGRESS)) {
            continue;
          }
          String znode = ledgerPath + "/" + child;
          EditLogLedgerMetadata l = EditLogLedgerMetadata.read(zkc, znode);
          try {
            long endTxId = recoverLastTxId(l, true);
            if (endTxId == HdfsConstants.INVALID_TXID) {
              LOG.error("Unrecoverable corruption has occurred in segment "
                  + l.toString() + " at path " + znode
                  + ". Unable to continue recovery.");
              throw new IOException("Unrecoverable corruption,"
                  + " please check logs.");
            }
            finalizeLogSegment(l.getFirstTxId(), endTxId);
          } catch (SegmentEmptyException see) {
            LOG.warn("Inprogress znode " + child
                + " refers to a ledger which is empty. This occurs when the NN"
                + " crashes after opening a segment, but before writing the"
                + " OP_START_LOG_SEGMENT op. It is safe to delete."
                + " MetaData [" + l.toString() + "]");

            // If the max seen transaction is the same as what would
            // have been the first transaction of the failed ledger,
            // decrement it, as that transaction never happened and as
            // such, is _not_ the last seen
            if (maxTxId.get() == l.getFirstTxId()) {
              maxTxId.reset(maxTxId.get() - 1);
            }

            zkc.delete(znode, -1);
          }
        }
      } catch (KeeperException.NoNodeException nne) {
          // nothing to recover, ignore
      } catch (KeeperException ke) {
        throw new IOException("Couldn't get list of inprogress segments", ke);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted getting list of inprogress segments",
                              ie);
      }
    }
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    checkEnv();

    for (EditLogLedgerMetadata l : getLedgerList(false)) {
      if (l.getLastTxId() < minTxIdToKeep) {
        try {
          Stat stat = zkc.exists(l.getZkPath(), false);
          zkc.delete(l.getZkPath(), stat.getVersion());
          bkc.deleteLedger(l.getLedgerId());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
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
  public void discardSegments(long startTxId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doPreUpgrade() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getJournalCTime() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doFinalize() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doRollback() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    try {
      bkc.close();
      zkc.close();
    } catch (BKException bke) {
      throw new IOException("Couldn't close bookkeeper client", bke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while closing journal manager", ie);
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
  private long recoverLastTxId(EditLogLedgerMetadata l, boolean fence)
      throws IOException, SegmentEmptyException {
    LedgerHandle lh = null;
    try {
      if (fence) {
        lh = bkc.openLedger(l.getLedgerId(),
                            BookKeeper.DigestType.MAC,
                            digestpw.getBytes(Charsets.UTF_8));
      } else {
        lh = bkc.openLedgerNoRecovery(l.getLedgerId(),
                                      BookKeeper.DigestType.MAC,
                                      digestpw.getBytes(Charsets.UTF_8));
      }
    } catch (BKException bke) {
      throw new IOException("Exception opening ledger for " + l, bke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted opening ledger for " + l, ie);
    }

    BookKeeperEditLogInputStream in = null;

    try {
      long lastAddConfirmed = lh.getLastAddConfirmed();
      if (lastAddConfirmed == -1) {
        throw new SegmentEmptyException();
      }

      in = new BookKeeperEditLogInputStream(lh, l, lastAddConfirmed);

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
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  /**
   * Get a list of all segments in the journal.
   */
  List<EditLogLedgerMetadata> getLedgerList(boolean inProgressOk)
      throws IOException {
    return getLedgerList(-1, inProgressOk);
  }

  private List<EditLogLedgerMetadata> getLedgerList(long fromTxId,
      boolean inProgressOk) throws IOException {
    List<EditLogLedgerMetadata> ledgers
      = new ArrayList<EditLogLedgerMetadata>();
    try {
      List<String> ledgerNames = zkc.getChildren(ledgerPath, false);
      for (String ledgerName : ledgerNames) {
        if (!inProgressOk && ledgerName.contains(BKJM_EDIT_INPROGRESS)) {
          continue;
        }
        String legderMetadataPath = ledgerPath + "/" + ledgerName;
        try {
          EditLogLedgerMetadata editLogLedgerMetadata = EditLogLedgerMetadata
              .read(zkc, legderMetadataPath);
          if (editLogLedgerMetadata.getLastTxId() != HdfsConstants.INVALID_TXID
              && editLogLedgerMetadata.getLastTxId() < fromTxId) {
            // exclude already read closed edits, but include inprogress edits
            // as this will be handled in caller
            continue;
          }
          ledgers.add(editLogLedgerMetadata);
        } catch (KeeperException.NoNodeException e) {
          LOG.warn("ZNode: " + legderMetadataPath
              + " might have finalized and deleted."
              + " So ignoring NoNodeException.");
        }
      }
    } catch (KeeperException e) {
      throw new IOException("Exception reading ledger list from zk", e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted getting list of ledgers from zk", ie);
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
  String inprogressZNode(long startTxid) {
    return ledgerPath + "/inprogress_" + Long.toString(startTxid, 16);
  }

  @VisibleForTesting
  void setZooKeeper(ZooKeeper zk) {
    this.zkc = zk;
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
  
  private static class SegmentEmptyException extends IOException {
  }
}
