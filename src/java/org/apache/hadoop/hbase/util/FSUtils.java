/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.dfs.DistributedFileSystem;

/**
 * Utility methods for interacting with the underlying file system.
 */
public class FSUtils {
  private static final Log LOG = LogFactory.getLog(FSUtils.class);

  /**
   * Not instantiable
   */
  private FSUtils() {super();}
  
  /**
   * Checks to see if the specified file system is available
   * 
   * @param fs
   * @param closed Optional flag.  If non-null and set, will abort test of
   * filesytem.  Presumption is a flag shared by multiple threads.  Another
   * may have already determined the filesystem -- or something else -- bad.
   * @return true if the specified file system is available.
   */
  public static boolean isFileSystemAvailable(final FileSystem fs,
      final AtomicBoolean closed) {
    if (!(fs instanceof DistributedFileSystem)) {
      return true;
    }
    boolean available = false;
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    int maxTries = dfs.getConf().getInt("hbase.client.retries.number", 3);
    Path root =
      fs.makeQualified(new Path(dfs.getConf().get(HConstants.HBASE_DIR, "/")));
    for (int i = 0; i < maxTries && (closed == null || !closed.get()); i++) {
      IOException ex = null;
      try {
        if (dfs.exists(root)) {
          available = true;
          break;
        }
      } catch (IOException e) {
        ex = e;
      }
      String exception = (ex == null)? "": ": " + ex.getMessage();
      LOG.info("Failed exists test on " + root + " by thread " +
        Thread.currentThread().getName() + " (Attempt " + i + " of " +
        maxTries  +"): " + exception);
    }
    try {
      if (!available) {
        fs.close();
      }
        
    } catch (IOException e) {
        LOG.error("file system close failed: ", e);
    }
    return available;
  }
}
