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
package org.apache.hadoop.hdfs.client;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

/**
 * The public API for performing administrative functions on HDFS. Those writing
 * applications against HDFS should prefer this interface to directly accessing
 * functionality in DistributedFileSystem or DFSClient.
 * 
 * Note that this is distinct from the similarly-named {@link DFSAdmin}, which
 * is a class that provides the functionality for the CLI `hdfs dfsadmin ...'
 * commands.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsAdmin {
  
  private DistributedFileSystem dfs;
  
  /**
   * Create a new HdfsAdmin client.
   * 
   * @param uri the unique URI of the HDFS file system to administer
   * @param conf configuration
   * @throws IOException in the event the file system could not be created
   */
  public HdfsAdmin(URI uri, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("'" + uri + "' is not an HDFS URI.");
    } else {
      dfs = (DistributedFileSystem)fs;
    }
  }
  
  /**
   * Set the namespace quota (count of files, directories, and sym links) for a
   * directory.
   * 
   * @param src the path to set the quota for
   * @param quota the value to set for the quota
   * @throws IOException in the event of error
   */
  public void setQuota(Path src, long quota) throws IOException {
    dfs.setQuota(src, quota, HdfsConstants.QUOTA_DONT_SET);
  }
  
  /**
   * Clear the namespace quota (count of files, directories and sym links) for a
   * directory.
   * 
   * @param src the path to clear the quota of
   * @throws IOException in the event of error
   */
  public void clearQuota(Path src) throws IOException {
    dfs.setQuota(src, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_DONT_SET);
  }
  
  /**
   * Set the disk space quota (size of files) for a directory. Note that
   * directories and sym links do not occupy disk space.
   * 
   * @param src the path to set the space quota of
   * @param spaceQuota the value to set for the space quota
   * @throws IOException in the event of error
   */
  public void setSpaceQuota(Path src, long spaceQuota) throws IOException {
    dfs.setQuota(src, HdfsConstants.QUOTA_DONT_SET, spaceQuota);
  }
  
  /**
   * Clear the disk space quota (size of files) for a directory. Note that
   * directories and sym links do not occupy disk space.
   * 
   * @param src the path to clear the space quota of
   * @throws IOException in the event of error
   */
  public void clearSpaceQuota(Path src) throws IOException {
    dfs.setQuota(src, HdfsConstants.QUOTA_DONT_SET, HdfsConstants.QUOTA_RESET);
  }
  
  /**
   * Allow snapshot on a directory.
   * @param path The path of the directory where snapshots will be taken.
   */
  public void allowSnapshot(Path path) throws IOException {
    dfs.allowSnapshot(path);
  }
  
  /**
   * Disallow snapshot on a directory.
   * @param path The path of the snapshottable directory.
   */
  public void disallowSnapshot(Path path) throws IOException {
    dfs.disallowSnapshot(path);
  }
}
