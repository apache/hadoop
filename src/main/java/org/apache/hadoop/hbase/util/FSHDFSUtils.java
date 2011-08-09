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

package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;


/**
 * Implementation for hdfs
 */
public class FSHDFSUtils extends FSUtils{
  private static final Log LOG = LogFactory.getLog(FSHDFSUtils.class);

  public void recoverFileLease(final FileSystem fs, final Path p, Configuration conf)
  throws IOException{
    if (!isAppendSupported(conf)) {
      LOG.warn("Running on HDFS without append enabled may result in data loss");
      return;
    }
    // lease recovery not needed for local file system case.
    // currently, local file system doesn't implement append either.
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    LOG.info("Recovering file " + p);
    long startWaiting = System.currentTimeMillis();

    // Trying recovery
    boolean recovered = false;
    while (!recovered) {
      try {
        try {
          if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem)fs;
            DistributedFileSystem.class.getMethod("recoverLease",
              new Class[] {Path.class}).invoke(dfs, p);
          } else {
            throw new Exception("Not a DistributedFileSystem");
          }
        } catch (InvocationTargetException ite) {
          // function was properly called, but threw it's own exception
          throw (IOException) ite.getCause();
        } catch (Exception e) {
          LOG.debug("Failed fs.recoverLease invocation, " + e.toString() +
            ", trying fs.append instead");
          FSDataOutputStream out = fs.append(p);
          out.close();
        }
        recovered = true;
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        if (e instanceof AlreadyBeingCreatedException) {
          // We expect that we'll get this message while the lease is still
          // within its soft limit, but if we get it past that, it means
          // that the RS is holding onto the file even though it lost its
          // znode. We could potentially abort after some time here.
          long waitedFor = System.currentTimeMillis() - startWaiting;
          if (waitedFor > FSConstants.LEASE_SOFTLIMIT_PERIOD) {
            LOG.warn("Waited " + waitedFor + "ms for lease recovery on " + p +
              ":" + e.getMessage());
          }
        } else if (e instanceof LeaseExpiredException &&
            e.getMessage().contains("File does not exist")) {
          // This exception comes out instead of FNFE, fix it
          throw new FileNotFoundException(
              "The given HLog wasn't found at " + p.toString());
        } else {
          throw new IOException("Failed to open " + p + " for append", e);
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        new InterruptedIOException().initCause(ex);
      }
    }
    LOG.info("Finished lease recover attempt for " + p);
  }
}
