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

import java.io.IOException;
import java.util.Comparator;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.contrib.bkjournal.BKJournalProtos.EditLogLedgerProto;
import com.google.protobuf.TextFormat;
import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing the metadata associated 
 * with a single edit log segment, stored in a single ledger
 */
public class EditLogLedgerMetadata {
  static final Log LOG = LogFactory.getLog(EditLogLedgerMetadata.class);

  private String zkPath;
  private final int dataLayoutVersion;
  private final long ledgerId;
  private final long firstTxId;
  private long lastTxId;
  private boolean inprogress;
  
  public static final Comparator COMPARATOR 
    = new Comparator<EditLogLedgerMetadata>() {
    public int compare(EditLogLedgerMetadata o1,
        EditLogLedgerMetadata o2) {
      if (o1.firstTxId < o2.firstTxId) {
        return -1;
      } else if (o1.firstTxId == o2.firstTxId) {
        return 0;
      } else {
        return 1;
      }
    }
  };

  EditLogLedgerMetadata(String zkPath, int dataLayoutVersion,
                        long ledgerId, long firstTxId) {
    this.zkPath = zkPath;
    this.dataLayoutVersion = dataLayoutVersion;
    this.ledgerId = ledgerId;
    this.firstTxId = firstTxId;
    this.lastTxId = HdfsConstants.INVALID_TXID;
    this.inprogress = true;
  }
  
  EditLogLedgerMetadata(String zkPath, int dataLayoutVersion,
                        long ledgerId, long firstTxId,
                        long lastTxId) {
    this.zkPath = zkPath;
    this.dataLayoutVersion = dataLayoutVersion;
    this.ledgerId = ledgerId;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    this.inprogress = false;
  }

  String getZkPath() {
    return zkPath;
  }

  long getFirstTxId() {
    return firstTxId;
  }
  
  long getLastTxId() {
    return lastTxId;
  }
  
  long getLedgerId() {
    return ledgerId;
  }
  
  boolean isInProgress() {
    return this.inprogress;
  }

  int getDataLayoutVersion() {
    return this.dataLayoutVersion;
  }

  void finalizeLedger(long newLastTxId) {
    assert this.lastTxId == HdfsConstants.INVALID_TXID;
    this.lastTxId = newLastTxId;
    this.inprogress = false;      
  }
  
  static EditLogLedgerMetadata read(ZooKeeper zkc, String path)
      throws IOException, KeeperException.NoNodeException  {
    try {
      byte[] data = zkc.getData(path, false, null);

      EditLogLedgerProto.Builder builder = EditLogLedgerProto.newBuilder();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading " + path + " data: " + new String(data, UTF_8));
      }
      TextFormat.merge(new String(data, UTF_8), builder);
      if (!builder.isInitialized()) {
        throw new IOException("Invalid/Incomplete data in znode");
      }
      EditLogLedgerProto ledger = builder.build();

      int dataLayoutVersion = ledger.getDataLayoutVersion();
      long ledgerId = ledger.getLedgerId();
      long firstTxId = ledger.getFirstTxId();
      if (ledger.hasLastTxId()) {
        long lastTxId = ledger.getLastTxId();
        return new EditLogLedgerMetadata(path, dataLayoutVersion,
                                         ledgerId, firstTxId, lastTxId);
      } else {
        return new EditLogLedgerMetadata(path, dataLayoutVersion,
                                         ledgerId, firstTxId);
      }
    } catch(KeeperException.NoNodeException nne) {
      throw nne;
    } catch(KeeperException ke) {
      throw new IOException("Error reading from zookeeper", ke);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted reading from zookeeper", ie);
    }
  }
    
  void write(ZooKeeper zkc, String path)
      throws IOException, KeeperException.NodeExistsException {
    this.zkPath = path;

    EditLogLedgerProto.Builder builder = EditLogLedgerProto.newBuilder();
    builder.setDataLayoutVersion(dataLayoutVersion)
      .setLedgerId(ledgerId).setFirstTxId(firstTxId);

    if (!inprogress) {
      builder.setLastTxId(lastTxId);
    }
    try {
      zkc.create(path, TextFormat.printToString(builder.build()).getBytes(UTF_8),
                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException nee) {
      throw nee;
    } catch (KeeperException e) {
      throw new IOException("Error creating ledger znode", e);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted creating ledger znode", ie);
    }
  }
  
  boolean verify(ZooKeeper zkc, String path) {
    try {
      EditLogLedgerMetadata other = read(zkc, path);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Verifying " + this.toString() 
                  + " against " + other);
      }
      return other.equals(this);
    } catch (KeeperException e) {
      LOG.error("Couldn't verify data in " + path, e);
      return false;
    } catch (IOException ie) {
      LOG.error("Couldn't verify data in " + path, ie);
      return false;
    }
  }
  
  public boolean equals(Object o) {
    if (!(o instanceof EditLogLedgerMetadata)) {
      return false;
    }
    EditLogLedgerMetadata ol = (EditLogLedgerMetadata)o;
    return ledgerId == ol.ledgerId
      && dataLayoutVersion == ol.dataLayoutVersion
      && firstTxId == ol.firstTxId
      && lastTxId == ol.lastTxId;
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * 31 + (int) ledgerId;
    hash = hash * 31 + (int) firstTxId;
    hash = hash * 31 + (int) lastTxId;
    hash = hash * 31 + dataLayoutVersion;
    return hash;
  }
    
  public String toString() {
    return "[LedgerId:"+ledgerId +
      ", firstTxId:" + firstTxId +
      ", lastTxId:" + lastTxId +
      ", dataLayoutVersion:" + dataLayoutVersion + "]";
  }

}
