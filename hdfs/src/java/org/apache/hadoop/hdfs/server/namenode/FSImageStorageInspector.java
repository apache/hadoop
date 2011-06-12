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
   * Create a plan to load the image from the set of inspected storage directories.
   * @throws IOException if not enough files are available (eg no image found in any directory)
   */
  abstract LoadPlan createLoadPlan() throws IOException;
  
  /**
   * @return true if the directories are in such a state that the image should be re-saved
   * following the load
   */
  abstract boolean needToSave();

  /**
   * A plan to load the namespace from disk, providing the locations from which to load
   * the image and a set of edits files.
   */
  abstract static class LoadPlan {
    /**
     * Execute atomic move sequence in the chosen storage directories,
     * in order to recover from an interrupted checkpoint.
     * @return true if some recovery action was taken
     */
    abstract boolean doRecovery() throws IOException;

    /**
     * @return the file from which to load the image data
     */
    abstract File getImageFile();
    
    /**
     * @return a list of flies containing edits to replay
     */
    abstract List<File> getEditsFiles();
    
    /**
     * @return the storage directory containing the VERSION file that should be
     * loaded.
     */
    abstract StorageDirectory getStorageDirectoryForProperties();
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Will load image file: ").append(getImageFile()).append("\n");
      sb.append("Will load edits files:").append("\n");
      for (File f : getEditsFiles()) {
        sb.append("  ").append(f).append("\n");
      }
      sb.append("Will load metadata from: ")
        .append(getStorageDirectoryForProperties())
        .append("\n");
      return sb.toString();
    }
  }
}
