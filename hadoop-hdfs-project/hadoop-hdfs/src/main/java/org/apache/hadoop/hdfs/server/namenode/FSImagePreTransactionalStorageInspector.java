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


import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.io.IOUtils;

/**
 * Inspects an FSImage storage directory in the "old" (pre-HDFS-1073) format.
 * This format has the following data files:
 *   - fsimage
 *   - fsimage.ckpt (when checkpoint is being uploaded)
 *   - edits
 *   - edits.new (when logs are "rolled")
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FSImagePreTransactionalStorageInspector extends FSImageStorageInspector {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSImagePreTransactionalStorageInspector.class);
  
  /* Flag if there is at least one storage dir that doesn't contain the newest
   * fstime */
  private boolean hasOutOfDateStorageDirs = false;
  /* Flag set false if there are any "previous" directories found */
  private boolean isUpgradeFinalized = true;
  private boolean needToSaveAfterRecovery = false;
  
  // Track the name and edits dir with the latest times
  private long latestNameCheckpointTime = Long.MIN_VALUE;
  private long latestEditsCheckpointTime = Long.MIN_VALUE;
  private StorageDirectory latestNameSD = null;
  private StorageDirectory latestEditsSD = null;

  /** Set to determine if all of storageDirectories share the same checkpoint */
  final Set<Long> checkpointTimes = new HashSet<Long>();

  private final List<String> imageDirs = new ArrayList<String>();
  private final List<String> editsDirs = new ArrayList<String>();
  
  @Override
  void inspectDirectory(StorageDirectory sd) throws IOException {
    // Was the file just formatted?
    if (!sd.getVersionFile().exists()) {
      hasOutOfDateStorageDirs = true;
      return;
    }
    
    boolean imageExists = false;
    boolean editsExists = false;
    
    // Determine if sd is image, edits or both
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
      imageExists = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE).exists();        
      imageDirs.add(sd.getRoot().getCanonicalPath());
    }
    
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
      editsExists = NNStorage.getStorageFile(sd, NameNodeFile.EDITS).exists();
      editsDirs.add(sd.getRoot().getCanonicalPath());
    }
    
    long checkpointTime = readCheckpointTime(sd);

    checkpointTimes.add(checkpointTime);
    
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE) && 
       (latestNameCheckpointTime < checkpointTime) && imageExists) {
      latestNameCheckpointTime = checkpointTime;
      latestNameSD = sd;
    }
    
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS) && 
         (latestEditsCheckpointTime < checkpointTime) && editsExists) {
      latestEditsCheckpointTime = checkpointTime;
      latestEditsSD = sd;
    }
    
    // check that we have a valid, non-default checkpointTime
    if (checkpointTime <= 0L)
      hasOutOfDateStorageDirs = true;
    
    // set finalized flag
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();    
  }

  /**
   * Determine the checkpoint time of the specified StorageDirectory
   *
   * @param sd StorageDirectory to check
   * @return If file exists and can be read, last checkpoint time. If not, 0L.
   * @throws IOException On errors processing file pointed to by sd
   */
  static long readCheckpointTime(StorageDirectory sd) throws IOException {
    File timeFile = NNStorage.getStorageFile(sd, NameNodeFile.TIME);
    long timeStamp = 0L;
    if (timeFile.exists() && FileUtil.canRead(timeFile)) {
      DataInputStream in = new DataInputStream(
          Files.newInputStream(timeFile.toPath()));
      try {
        timeStamp = in.readLong();
        in.close();
        in = null;
      } finally {
        IOUtils.cleanupWithLogger(LOG, in);
      }
    }
    return timeStamp;
  }

  @Override
  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
    
  @Override
  List<FSImageFile> getLatestImages() throws IOException {
    // We should have at least one image and one edits dirs
    if (latestNameSD == null)
      throw new IOException("Image file is not found in " + imageDirs);
    if (latestEditsSD == null)
      throw new IOException("Edits file is not found in " + editsDirs);
    
    // Make sure we are loading image and edits from same checkpoint
    if (latestNameCheckpointTime > latestEditsCheckpointTime
        && latestNameSD != latestEditsSD
        && latestNameSD.getStorageDirType() == NameNodeDirType.IMAGE
        && latestEditsSD.getStorageDirType() == NameNodeDirType.EDITS) {
      // This is a rare failure when NN has image-only and edits-only
      // storage directories, and fails right after saving images,
      // in some of the storage directories, but before purging edits.
      // See -NOTE- in saveNamespace().
      LOG.error("This is a rare failure scenario!!!");
      LOG.error("Image checkpoint time " + latestNameCheckpointTime +
                " > edits checkpoint time " + latestEditsCheckpointTime);
      LOG.error("Name-node will treat the image as the latest state of " +
                "the namespace. Old edits will be discarded.");
    } else if (latestNameCheckpointTime != latestEditsCheckpointTime) {
      throw new IOException("Inconsistent storage detected, " +
                      "image and edits checkpoint times do not match. " +
                      "image checkpoint time = " + latestNameCheckpointTime +
                      "edits checkpoint time = " + latestEditsCheckpointTime);
    }

    needToSaveAfterRecovery = doRecovery();
    
    FSImageFile file = new FSImageFile(latestNameSD, 
        NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE),
        HdfsServerConstants.INVALID_TXID);
    LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();
    ret.add(file);
    return ret;
  }

  @Override
  boolean needToSave() {
    return hasOutOfDateStorageDirs ||
      checkpointTimes.size() != 1 ||
      latestNameCheckpointTime > latestEditsCheckpointTime ||
      needToSaveAfterRecovery;
  }
  
  boolean doRecovery() throws IOException {
    LOG.debug(
        "Performing recovery in "+ latestNameSD + " and " + latestEditsSD);
      
    boolean needToSave = false;
    File curFile =
      NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE);
    File ckptFile =
      NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE_NEW);
    
    //
    // If we were in the midst of a checkpoint
    //
    if (ckptFile.exists()) {
      needToSave = true;
      if (NNStorage.getStorageFile(latestEditsSD, NameNodeFile.EDITS_NEW)
          .exists()) {
        //
        // checkpointing migth have uploaded a new
        // merged image, but we discard it here because we are
        // not sure whether the entire merged image was uploaded
        // before the namenode crashed.
        //
        if (!ckptFile.delete()) {
          throw new IOException("Unable to delete " + ckptFile);
        }
      } else {
        //
        // checkpointing was in progress when the namenode
        // shutdown. The fsimage.ckpt was created and the edits.new
        // file was moved to edits. We complete that checkpoint by
        // moving fsimage.new to fsimage. There is no need to 
        // update the fstime file here. renameTo fails on Windows
        // if the destination file already exists.
        //
        if (!ckptFile.renameTo(curFile)) {
          if (!curFile.delete())
            LOG.warn("Unable to delete dir " + curFile + " before rename");
          if (!ckptFile.renameTo(curFile)) {
            throw new IOException("Unable to rename " + ckptFile +
                                  " to " + curFile);
          }
        }
      }
    }
    return needToSave;
  }
  
  /**
   * @return a list with the paths to EDITS and EDITS_NEW (if it exists)
   * in a given storage directory.
   */
  static List<File> getEditsInStorageDir(StorageDirectory sd) {
    ArrayList<File> files = new ArrayList<File>();
    File edits = NNStorage.getStorageFile(sd, NameNodeFile.EDITS);
    assert edits.exists() : "Expected edits file at " + edits;
    files.add(edits);
    File editsNew = NNStorage.getStorageFile(sd, NameNodeFile.EDITS_NEW);
    if (editsNew.exists()) {
      files.add(editsNew);
    }
    return files;
  }
  
  private List<File> getLatestEditsFiles() {
    if (latestNameCheckpointTime > latestEditsCheckpointTime) {
      // the image is already current, discard edits
      LOG.debug(
          "Name checkpoint time is newer than edits, not loading edits.");
      return Collections.emptyList();
    }
    
    return getEditsInStorageDir(latestEditsSD);
  }
  
  @Override
  long getMaxSeenTxId() {
    return 0L;
  }

  static Iterable<EditLogInputStream> getEditLogStreams(NNStorage storage)
      throws IOException {
    FSImagePreTransactionalStorageInspector inspector 
      = new FSImagePreTransactionalStorageInspector();
    storage.inspectStorageDirs(inspector);

    List<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
    for (File f : inspector.getLatestEditsFiles()) {
      editStreams.add(new EditLogFileInputStream(f));
    }
    return editStreams;
  }
}
