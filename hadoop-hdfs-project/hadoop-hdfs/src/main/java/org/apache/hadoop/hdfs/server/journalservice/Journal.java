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
package org.apache.hadoop.hdfs.server.journalservice;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

/** The journal stored in local directories. */
class Journal {
  static final Log LOG = LogFactory.getLog(Journal.class);

  private final FSImage image;
  private volatile boolean isFormatted;

  /**
   * Constructor. It is possible that the directory is not formatted.
   */
  Journal(Configuration conf) throws IOException {
    this.image = new FSImage(conf, Collections.<URI>emptyList(), getEditDirs(conf));

    final Map<StorageDirectory, StorageState> states
        = new HashMap<StorageDirectory, StorageState>();
    isFormatted = image.recoverStorageDirs(StartupOption.REGULAR, states);
    for(Map.Entry<StorageDirectory, StorageState> e : states.entrySet()) {
      LOG.info(e.getKey() + ": " + e.getValue());
    }
    LOG.info("The journal is " + (isFormatted? "already": "not yet")
        + " formatted: " + image.getStorage());
  }

  /**
   * Format the local edit directory.
   */
  synchronized void format(int namespaceId, String clusterId) throws IOException {
    if (isFormatted) {
      throw new IllegalStateException("The joural is already formatted.");
    }
    final NNStorage s = image.getStorage(); 
    s.format(new NamespaceInfo(namespaceId, clusterId, "dummy-bpid", 0, 0));
    isFormatted = true;
    LOG.info("Formatted journal: " + s);
  }

  boolean isFormatted() {
    return isFormatted;
  }
  
  NNStorage getStorage() {
    return image.getStorage();
  }
  
  synchronized void verifyVersion(JournalService service, NamespaceInfo info
      ) throws IOException {
    if (!isFormatted) {
      return;
    }

    final StorageInfo stored = image.getStorage();
    if (!stored.getClusterID().equals(info.getClusterID())) {
      throw new IOException("Cluster IDs not matched: stored = "
          + stored.getClusterID() + " != passed = " + info.getClusterID());
    }
    if (stored.getNamespaceID() != info.getNamespaceID()) {
      throw new IOException("Namespace IDs not matched: stored = "
          + stored.getNamespaceID() + " != passed = " + info.getNamespaceID());
    }
    if (stored.getLayoutVersion() != info.getLayoutVersion()) {
      throw new IOException("Layout versions not matched: stored = "
          + stored.getLayoutVersion() + " != passed = " + info.getLayoutVersion());
    }
    if (stored.getCTime() != info.getCTime()) {
      throw new IOException("CTimes not matched: stored = "
          + stored.getCTime() + " != passed = " + info.getCTime());
    }
  }

  void close() throws IOException {
    image.close();
  }

  FSEditLog getEditLog() {
    return image.getEditLog();
  }
  
  RemoteEditLogManifest getRemoteEditLogs(long sinceTxId) throws IOException {
    return image.getEditLog().getEditLogManifest(sinceTxId);
  }
  
  static List<URI> getEditDirs(Configuration conf) throws IOException {
    final Collection<String> dirs = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY);
    LOG.info(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY + " = " + dirs);
    return Util.stringCollectionAsURIs(dirs);
  }
}