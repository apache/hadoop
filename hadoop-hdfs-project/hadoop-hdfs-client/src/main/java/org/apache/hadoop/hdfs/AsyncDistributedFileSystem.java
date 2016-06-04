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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.ipc.Client;

import com.google.common.util.concurrent.AbstractFuture;

/****************************************************************
 * Implementation of the asynchronous distributed file system.
 * This instance of this class is the way end-user code interacts
 * with a Hadoop DistributedFileSystem in an asynchronous manner.
 *
 *****************************************************************/
@Unstable
public class AsyncDistributedFileSystem {

  private final DistributedFileSystem dfs;

  AsyncDistributedFileSystem(final DistributedFileSystem dfs) {
    this.dfs = dfs;
  }

  static <T> Future<T> getReturnValue() {
    final Callable<T> returnValueCallback = ClientNamenodeProtocolTranslatorPB
        .getReturnValueCallback();
    Future<T> returnFuture = new AbstractFuture<T>() {
      public T get() throws InterruptedException, ExecutionException {
        try {
          set(returnValueCallback.call());
        } catch (Exception e) {
          setException(e);
        }
        return super.get();
      }
    };
    return returnFuture;
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
}
