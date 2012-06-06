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

/**
 * Utility class for storing the metadata associated 
 * with a single edit log segment, stored in a single ledger
 */
public class EditLogLedgerMetadata {
  static final Log LOG = LogFactory.getLog(EditLogLedgerMetadata.class);

  private String zkPath;
  private final long ledgerId;
  private final int version;
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

  EditLogLedgerMetadata(String zkPath, int version, 
                        long ledgerId, long firstTxId) {
    this.zkPath = zkPath;
    this.ledgerId = ledgerId;
    this.version = version;
    this.firstTxId = firstTxId;
    this.lastTxId = HdfsConstants.INVALID_TXID;
    this.inprogress = true;
  }
  
  EditLogLedgerMetadata(String zkPath, int version, long ledgerId, 
                        long firstTxId, long lastTxId) {
    this.zkPath = zkPath;
    this.ledgerId = ledgerId;
    this.version = version;
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
  
  int getVersion() {
    return version;
  }

  boolean isInProgress() {
    return this.inprogress;
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
      String[] parts = new String(data).split(";");
      if (parts.length == 3) {
        int version = Integer.valueOf(parts[0]);
        long ledgerId = Long.valueOf(parts[1]);
        long txId = Long.valueOf(parts[2]);
        return new EditLogLedgerMetadata(path, version, ledgerId, txId);
      } else if (parts.length == 4) {
        int version = Integer.valueOf(parts[0]);
        long ledgerId = Long.valueOf(parts[1]);
        long firstTxId = Long.valueOf(parts[2]);
        long lastTxId = Long.valueOf(parts[3]);
        return new EditLogLedgerMetadata(path, version, ledgerId,
                                         firstTxId, lastTxId);
      } else {
        throw new IOException("Invalid ledger entry, "
                              + new String(data));
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
    String finalisedData;
    if (inprogress) {
      finalisedData = String.format("%d;%d;%d",
          version, ledgerId, firstTxId);
    } else {
      finalisedData = String.format("%d;%d;%d;%d",
          version, ledgerId, firstTxId, lastTxId);
    }
    try {
      zkc.create(path, finalisedData.getBytes(), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
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
      && firstTxId == ol.firstTxId
      && lastTxId == ol.lastTxId
      && version == ol.version;
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * 31 + (int) ledgerId;
    hash = hash * 31 + (int) firstTxId;
    hash = hash * 31 + (int) lastTxId;
    hash = hash * 31 + (int) version;
    return hash;
  }
    
  public String toString() {
    return "[LedgerId:"+ledgerId +
      ", firstTxId:" + firstTxId +
      ", lastTxId:" + lastTxId + 
      ", version:" + version + "]";
  }

}
