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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import com.google.common.collect.ImmutableList;

class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
  public static final Log LOG = LogFactory.getLog(
    FSImageTransactionalStorageInspector.class);

  private boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  List<FSImageFile> foundImages = new ArrayList<FSImageFile>();
  private long maxSeenTxId = 0;
  
  private static final Pattern IMAGE_REGEX = Pattern.compile(
    NameNodeFile.IMAGE.getName() + "_(\\d+)");

  @Override
  public void inspectDirectory(StorageDirectory sd) throws IOException {
    // Was the directory just formatted?
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    maxSeenTxId = Math.max(maxSeenTxId, NNStorage.readTransactionIdFile(sd));

    File currentDir = sd.getCurrentDir();
    File filesInStorage[];
    try {
      filesInStorage = FileUtil.listFiles(currentDir);
    } catch (IOException ioe) {
      LOG.warn("Unable to inspect storage directory " + currentDir,
          ioe);
      return;
    }

    for (File f : filesInStorage) {
      LOG.debug("Checking file " + f);
      String name = f.getName();
      
      // Check for fsimage_*
      Matcher imageMatch = IMAGE_REGEX.matcher(name);
      if (imageMatch.matches()) {
        if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
          try {
            long txid = Long.valueOf(imageMatch.group(1));
            foundImages.add(new FSImageFile(sd, f, txid));
          } catch (NumberFormatException nfe) {
            LOG.error("Image file " + f + " has improperly formatted " +
                      "transaction ID");
            // skip
          }
        } else {
          LOG.warn("Found image file at " + f + " but storage directory is " +
                   "not configured to contain images.");
        }
      }
    }
    

    // Check for a seen_txid file, which marks a minimum transaction ID that
    // must be included in our load plan.
    try {
      maxSeenTxId = Math.max(maxSeenTxId, NNStorage.readTransactionIdFile(sd));
    } catch (IOException ioe) {
      LOG.warn("Unable to determine the max transaction ID seen by " + sd, ioe);
    }
    
    // set finalized flag
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }

  @Override
  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /**
   * @return the image that has the most recent associated transaction ID.
   * If there are multiple storage directories which contain equal images 
   * the storage directory that was inspected first will be preferred.
   * 
   * @throws FileNotFoundException if not images are found.
   */
  FSImageFile getLatestImage() throws IOException {
    if (foundImages.isEmpty()) {
      throw new FileNotFoundException("No valid image files found");
    }

    FSImageFile ret = null;
    for (FSImageFile img : foundImages) {
      if (ret == null || img.txId > ret.txId) {
        ret = img;
      }
    }
    return ret;
  }
  
  public List<FSImageFile> getFoundImages() {
    return ImmutableList.copyOf(foundImages);
  }
  
  @Override
  public boolean needToSave() {
    return needToSave;
  }

  @Override
  long getMaxSeenTxId() {
    return maxSeenTxId;
  }
}
