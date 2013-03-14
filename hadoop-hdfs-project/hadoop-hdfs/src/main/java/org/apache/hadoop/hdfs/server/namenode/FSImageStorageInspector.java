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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * Interface responsible for inspecting a set of storage directories and devising
 * a plan to load the namespace from them.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract class FSImageStorageInspector {
  /**
   * Inspect the contents of the given storage directory.
   */
  abstract void inspectDirectory(StorageDirectory sd) throws IOException;

  /**
   * @return false if any of the storage directories have an unfinalized upgrade 
   */
  abstract boolean isUpgradeFinalized();
  
  /**
   * Get the image files which should be loaded into the filesystem.
   * @throws IOException if not enough files are available (eg no image found in any directory)
   */
  abstract List<FSImageFile> getLatestImages() throws IOException;

  /** 
   * Get the minimum tx id which should be loaded with this set of images.
   */
  abstract long getMaxSeenTxId();

  /**
   * @return true if the directories are in such a state that the image should be re-saved
   * following the load
   */
  abstract boolean needToSave();

  /**
   * Record of an image that has been located and had its filename parsed.
   */
  static class FSImageFile {
    final StorageDirectory sd;    
    final long txId;
    private final File file;
    
    FSImageFile(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 || txId == HdfsConstants.INVALID_TXID 
        : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
    } 
    
    File getFile() {
      return file;
    }

    public long getCheckpointTxId() {
      return txId;
    }
    
    @Override
    public String toString() {
      return String.format("FSImageFile(file=%s, cpktTxId=%019d)", 
                           file.toString(), txId);
    }
  }

}
