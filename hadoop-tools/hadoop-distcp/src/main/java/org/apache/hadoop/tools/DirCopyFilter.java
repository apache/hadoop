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
package org.apache.hadoop.tools;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A CopyFilter which checks if the FileStatus is a file or directory, the directory
 * will be kept and the file will be filtered out.
 */
public class DirCopyFilter extends FileStatusCopyFilter {
  private static final Logger LOG = LoggerFactory.getLogger(DirCopyFilter.class);
  private Configuration conf;

  /**
   * Constructor of DirCopyFilter, it can be instantiated by CopyFilter#getCopyFilter method.
   * @param conf Configuration.
   */
  public DirCopyFilter(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean shouldCopy(Path path) {
    try {
      FileSystem fs = path.getFileSystem(this.conf);
      if (fs.getFileStatus(path).isDirectory()) {
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception occurred when get FileSystem or get FileStatus", e);
    }

    LOG.debug("Skipping {} as it is not a directory", path);
    return false;
  }

  @Override
  public boolean shouldCopy(CopyListingFileStatus fileStatus) {
    return fileStatus.isDirectory();
  }
}
