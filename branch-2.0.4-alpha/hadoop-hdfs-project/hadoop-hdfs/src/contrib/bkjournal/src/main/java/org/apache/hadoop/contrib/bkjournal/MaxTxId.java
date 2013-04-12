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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import org.apache.hadoop.contrib.bkjournal.BKJournalProtos.MaxTxIdProto;
import com.google.protobuf.TextFormat;
import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing and reading
 * the max seen txid in zookeeper
 */
class MaxTxId {
  static final Log LOG = LogFactory.getLog(MaxTxId.class);
  
  private final ZooKeeper zkc;
  private final String path;

  private Stat currentStat;

  MaxTxId(ZooKeeper zkc, String path) {
    this.zkc = zkc;
    this.path = path;
  }

  synchronized void store(long maxTxId) throws IOException {
    long currentMax = get();
    if (currentMax < maxTxId) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Setting maxTxId to " + maxTxId);
      }
      reset(maxTxId);
    }
  }

  synchronized void reset(long maxTxId) throws IOException {
    try {
      MaxTxIdProto.Builder builder = MaxTxIdProto.newBuilder().setTxId(maxTxId);

      byte[] data = TextFormat.printToString(builder.build()).getBytes(UTF_8);
      if (currentStat != null) {
        currentStat = zkc.setData(path, data, currentStat
            .getVersion());
      } else {
        zkc.create(path, data, Ids.OPEN_ACL_UNSAFE,
                   CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      throw new IOException("Error writing max tx id", e);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while writing max tx id", e);
    }
  }

  synchronized long get() throws IOException {
    try {
      currentStat = zkc.exists(path, false);
      if (currentStat == null) {
        return 0;
      } else {

        byte[] bytes = zkc.getData(path, false, currentStat);

        MaxTxIdProto.Builder builder = MaxTxIdProto.newBuilder();
        TextFormat.merge(new String(bytes, UTF_8), builder);
        if (!builder.isInitialized()) {
          throw new IOException("Invalid/Incomplete data in znode");
        }

        return builder.build().getTxId();
      }
    } catch (KeeperException e) {
      throw new IOException("Error reading the max tx id from zk", e);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted while reading thr max tx id", ie);
    }
  }
}
