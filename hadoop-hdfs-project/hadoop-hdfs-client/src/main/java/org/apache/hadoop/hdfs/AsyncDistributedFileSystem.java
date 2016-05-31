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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.io.retry.AsyncCallHandler;
import org.apache.hadoop.util.concurrent.AsyncGetFuture;
import org.apache.hadoop.ipc.Client;

/****************************************************************
 * Implementation of the asynchronous distributed file system.
 * This instance of this class is the way end-user code interacts
 * with a Hadoop DistributedFileSystem in an asynchronous manner.
 *
 * This class is unstable, so no guarantee is provided as to reliability,
 * stability or compatibility across any level of release granularity.
 *
 *****************************************************************/
@Unstable
public class AsyncDistributedFileSystem {

  private final DistributedFileSystem dfs;

  AsyncDistributedFileSystem(final DistributedFileSystem dfs) {
    this.dfs = dfs;
  }

  private static <T> Future<T> getReturnValue() {
    return new AsyncGetFuture<>(AsyncCallHandler.getAsyncReturn());
  }

  /**
   * Renames Path src to Path dst
   * <ul>
   * <li>Fails if src is a file and dst is a directory.
   * <li>Fails if src is a directory and dst is a file.
   * <li>Fails if the parent of dst does not exist or is a file.
   * </ul>
   * <p>
   * If OVERWRITE option is not passed as an argument, rename fails if the dst
   * already exists.
   * <p>
   * If OVERWRITE option is passed as an argument, rename overwrites the dst if
   * it is a file or an empty directory. Rename fails if dst is a non-empty
   * directory.
   * <p>
   * Note that atomicity of rename is dependent on the file system
   * implementation. Please refer to the file system documentation for details.
   * This default implementation is non atomic.
   *
   * @param src
   *          path to be renamed
   * @param dst
   *          new path after rename
   * @throws IOException
   *           on failure
   * @return an instance of Future, #get of which is invoked to wait for
   *         asynchronous call being finished.
   */
  public Future<Void> rename(Path src, Path dst,
      final Options.Rename... options) throws IOException {
    dfs.getFsStatistics().incrementWriteOps(1);
    dfs.getDFSOpsCountStatistics().incrementOpCounter(OpType.RENAME);

    final Path absSrc = dfs.fixRelativePart(src);
    final Path absDst = dfs.fixRelativePart(dst);

    final boolean isAsync = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
    try {
      dfs.getClient().rename(dfs.getPathName(absSrc), dfs.getPathName(absDst),
          options);
      return getReturnValue();
    } finally {
      Client.setAsynchronousMode(isAsync);
    }
  }

  /**
   * Set permission of a path.
   *
   * @param p
   *          the path the permission is set to
   * @param permission
   *          the permission that is set to a path.
   * @return an instance of Future, #get of which is invoked to wait for
   *         asynchronous call being finished.
   */
  public Future<Void> setPermission(Path p, final FsPermission permission)
      throws IOException {
    dfs.getFsStatistics().incrementWriteOps(1);
    dfs.getDFSOpsCountStatistics().incrementOpCounter(OpType.SET_PERMISSION);
    final Path absPath = dfs.fixRelativePart(p);
    final boolean isAsync = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
    try {
      dfs.getClient().setPermission(dfs.getPathName(absPath), permission);
      return getReturnValue();
    } finally {
      Client.setAsynchronousMode(isAsync);
    }
  }

  /**
   * Set owner of a path (i.e. a file or a directory). The parameters username
   * and groupname cannot both be null.
   *
   * @param p
   *          The path
   * @param username
   *          If it is null, the original username remains unchanged.
   * @param groupname
   *          If it is null, the original groupname remains unchanged.
   * @return an instance of Future, #get of which is invoked to wait for
   *         asynchronous call being finished.
   */
  public Future<Void> setOwner(Path p, String username, String groupname)
      throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }

    dfs.getFsStatistics().incrementWriteOps(1);
    dfs.getDFSOpsCountStatistics().incrementOpCounter(OpType.SET_OWNER);
    final Path absPath = dfs.fixRelativePart(p);
    final boolean isAsync = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
    try {
      dfs.getClient().setOwner(dfs.getPathName(absPath), username, groupname);
      return getReturnValue();
    } finally {
      Client.setAsynchronousMode(isAsync);
    }
  }

  /**
   * Fully replaces ACL of files and directories, discarding all existing
   * entries.
   *
   * @param p
   *          Path to modify
   * @param aclSpec
   *          List<AclEntry> describing modifications, must include entries for
   *          user, group, and others for compatibility with permission bits.
   * @throws IOException
   *           if an ACL could not be modified
   * @return an instance of Future, #get of which is invoked to wait for
   *         asynchronous call being finished.
   */
  public Future<Void> setAcl(Path p, final List<AclEntry> aclSpec)
      throws IOException {
    dfs.getFsStatistics().incrementWriteOps(1);
    dfs.getDFSOpsCountStatistics().incrementOpCounter(OpType.SET_ACL);
    final Path absPath = dfs.fixRelativePart(p);
    final boolean isAsync = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
    try {
      dfs.getClient().setAcl(dfs.getPathName(absPath), aclSpec);
      return getReturnValue();
    } finally {
      Client.setAsynchronousMode(isAsync);
    }
  }

  /**
   * Gets the ACL of a file or directory.
   *
   * @param p
   *          Path to get
   * @return AclStatus describing the ACL of the file or directory
   * @throws IOException
   *           if an ACL could not be read
   * @return an instance of Future, #get of which is invoked to wait for
   *         asynchronous call being finished.
   */
  public Future<AclStatus> getAclStatus(Path p) throws IOException {
    final Path absPath = dfs.fixRelativePart(p);
    final boolean isAsync = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
    try {
      dfs.getClient().getAclStatus(dfs.getPathName(absPath));
      return getReturnValue();
    } finally {
      Client.setAsynchronousMode(isAsync);
    }
  }
}
