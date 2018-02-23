/*
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

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.model.OSSObjectSummary;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.objectRepresentsDirectory;

/**
 * Interface to implement by the logic deciding whether to accept a summary
 * entry or path as a valid file or directory.
 */
public interface FileStatusAcceptor {

  /**
   * Predicate to decide whether or not to accept a summary entry.
   * @param keyPath qualified path to the entry
   * @param summary summary entry
   * @return true if the entry is accepted (i.e. that a status entry
   * should be generated.
   */
  boolean accept(Path keyPath, OSSObjectSummary summary);

  /**
   * Predicate to decide whether or not to accept a prefix.
   * @param keyPath qualified path to the entry
   * @param commonPrefix the prefix
   * @return true if the entry is accepted (i.e. that a status entry
   * should be generated.)
   */
  boolean accept(Path keyPath, String commonPrefix);

  /**
   * Accept all entries except the base path.
   */
  class AcceptFilesOnly implements FileStatusAcceptor {
    private final Path qualifiedPath;

    public AcceptFilesOnly(Path qualifiedPath) {
      this.qualifiedPath = qualifiedPath;
    }

    /**
     * Reject a summary entry if the key path is the qualified Path.
     * @param keyPath key path of the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.
     */
    @Override
    public boolean accept(Path keyPath, OSSObjectSummary summary) {
      return !keyPath.equals(qualifiedPath)
        && !objectRepresentsDirectory(summary.getKey(), summary.getSize());
    }

    /**
     * Accept no directory paths.
     * @param keyPath qualified path to the entry
     * @param prefix common prefix in listing.
     * @return false, always.
     */
    @Override
    public boolean accept(Path keyPath, String prefix) {
      return false;
    }
  }

  /**
   * Accept all entries except the base path.
   */
  class AcceptAllButSelf implements FileStatusAcceptor {

    /** Base path. */
    private final Path qualifiedPath;

    /**
     * Constructor.
     * @param qualifiedPath an already-qualified path.
     */
    public AcceptAllButSelf(Path qualifiedPath) {
      this.qualifiedPath = qualifiedPath;
    }

    /**
     * Reject a summary entry if the key path is the qualified Path.
     * @param keyPath key path of the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.)
     */
    @Override
    public boolean accept(Path keyPath, OSSObjectSummary summary) {
      return !keyPath.equals(qualifiedPath);
    }

    /**
     * Accept all prefixes except the one for the base path, "self".
     * @param keyPath qualified path to the entry
     * @param prefix common prefix in listing.
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.
     */
    @Override
    public boolean accept(Path keyPath, String prefix) {
      return !keyPath.equals(qualifiedPath);
    }
  }
}