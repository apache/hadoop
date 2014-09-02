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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

/**
 * The class provides utilities for key and token management.
 */
@InterfaceAudience.Private
public class KeyManager implements Closeable, DataEncryptionKeyFactory {
  private static final Log LOG = LogFactory.getLog(KeyManager.class);

  private final NamenodeProtocol namenode;

  private final boolean isBlockTokenEnabled;
  private final boolean encryptDataTransfer;
  private boolean shouldRun;

  private final BlockTokenSecretManager blockTokenSecretManager;
  private final BlockKeyUpdater blockKeyUpdater;
  private DataEncryptionKey encryptionKey;

  public KeyManager(String blockpoolID, NamenodeProtocol namenode,
      boolean encryptDataTransfer, Configuration conf) throws IOException {
    this.namenode = namenode;
    this.encryptDataTransfer = encryptDataTransfer;

    final ExportedBlockKeys keys = namenode.getBlockKeys();
    this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
    if (isBlockTokenEnabled) {
      long updateInterval = keys.getKeyUpdateInterval();
      long tokenLifetime = keys.getTokenLifetime();
      LOG.info("Block token params received from NN: update interval="
          + StringUtils.formatTime(updateInterval)
          + ", token lifetime=" + StringUtils.formatTime(tokenLifetime));
      String encryptionAlgorithm = conf.get(
          DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
      this.blockTokenSecretManager = new BlockTokenSecretManager(
          updateInterval, tokenLifetime, blockpoolID, encryptionAlgorithm);
      this.blockTokenSecretManager.addKeys(keys);

      // sync block keys with NN more frequently than NN updates its block keys
      this.blockKeyUpdater = new BlockKeyUpdater(updateInterval / 4);
      this.shouldRun = true;
    } else {
      this.blockTokenSecretManager = null;
      this.blockKeyUpdater = null;
    }
  }
  
  public void startBlockKeyUpdater() {
    if (blockKeyUpdater != null) {
      blockKeyUpdater.daemon.start();
    }
  }

  /** Get an access token for a block. */
  public Token<BlockTokenIdentifier> getAccessToken(ExtendedBlock eb
      ) throws IOException {
    if (!isBlockTokenEnabled) {
      return BlockTokenSecretManager.DUMMY_TOKEN;
    } else {
      if (!shouldRun) {
        throw new IOException(
            "Cannot get access token since BlockKeyUpdater is not running");
      }
      return blockTokenSecretManager.generateToken(null, eb,
          EnumSet.of(AccessMode.REPLACE, AccessMode.COPY));
    }
  }

  @Override
  public DataEncryptionKey newDataEncryptionKey() {
    if (encryptDataTransfer) {
      synchronized (this) {
        if (encryptionKey == null) {
          encryptionKey = blockTokenSecretManager.generateDataEncryptionKey();
        }
        return encryptionKey;
      }
    } else {
      return null;
    }
  }

  @Override
  public void close() {
    shouldRun = false;
    try {
      if (blockKeyUpdater != null) {
        blockKeyUpdater.daemon.interrupt();
      }
    } catch(Exception e) {
      LOG.warn("Exception shutting down access key updater thread", e);
    }
  }

  /**
   * Periodically updates access keys.
   */
  class BlockKeyUpdater implements Runnable, Closeable {
    private final Daemon daemon = new Daemon(this);
    private final long sleepInterval;

    BlockKeyUpdater(final long sleepInterval) {
      this.sleepInterval = sleepInterval;
      LOG.info("Update block keys every " + StringUtils.formatTime(sleepInterval));
    }

    @Override
    public void run() {
      try {
        while (shouldRun) {
          try {
            blockTokenSecretManager.addKeys(namenode.getBlockKeys());
          } catch (IOException e) {
            LOG.error("Failed to set keys", e);
          }
          Thread.sleep(sleepInterval);
        }
      } catch (InterruptedException e) {
        LOG.debug("InterruptedException in block key updater thread", e);
      } catch (Throwable e) {
        LOG.error("Exception in block key updater thread", e);
        shouldRun = false;
      }
    }

    @Override
    public void close() throws IOException {
      try {
        daemon.interrupt();
      } catch(Exception e) {
        LOG.warn("Exception shutting down key updater thread", e);
      }
    }
  }
}
