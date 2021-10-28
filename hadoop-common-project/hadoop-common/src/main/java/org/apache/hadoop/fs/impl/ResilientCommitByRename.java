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

package org.apache.hadoop.fs.impl;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * This is something internal to make our rename-based job committers
 * more resilient to failures.
 * If you are in the hive team: do not use this as it lacks
 * spec, tests, stability, etc. if we find you using it we will change
 * the signature just to stop your code compiling.
 * View this as a proof of concept of the functionality we'd want from a
 * "modern" rename call, but not the API (which would be builder based,
 * return a future, etc).
 */
@InterfaceAudience.LimitedPrivate({"Filesystems", "hadoop-mapreduce-client-core"})
@InterfaceStability.Unstable
public interface ResilientCommitByRename {

  /**
   * Path capability.
   * FS Instances which support the operation MUST return
   * true and implement the method; FileSystem instances which do not
   * MUST return false.
   * There's a risk wrapper filesystems may pass the probe
   * through.
   * Clients MUST check for both the interface and this
   * cqpability.
   */
  String RESILIENT_COMMIT_BY_RENAME_PATH_CAPABILITY =
      "org.apache.hadoop.fs.impl.resilient.commit.by.rename";

  /**
   * Rename source file to dest path *Exactly*; no subdirectory games here.
   * if the op does not raise an exception,then
   * the data at dest is the data which was at source.
   *
   * Requirements
   *
   * <pre>
   *   exists(FS, source) else raise FileNotFoundException
   *   source != dest else raise PathIOException
   *   not exists(FS, dest)
   *   isDir(FS, dest.getParent)
   * </pre>
   * <ol>
   *   <li>supported in this instance else raise PathIOException</li>
   *   <li>source != dest else raise PathIOException</li>
   *   <li>source must exist else raise FileNotFoundException</li>
   *   <li>source must exist and be a file</li>
   *   <li>dest must not exist; </li>
   *   <li>dest.getParent() must be a dir</li>
   *   <li>if sourceEtag is non-empty, it MAY be used to qualify/validate the rename.</li>
   * </ol>
   *
   * The outcome of the operation is undefined if source is not a file, dest exists,
   * dest.getParent() doesn't exist/is a file.
   * That is: implementations SHOULD assume that the code calling this method has
   * set up the destination directory tree and is only invoking this call on a file.
   * Accordingly: <i>implementations MAY skip validation checks</i>
   *
   * If sourceStatus is not null, its contents MAY be used to qualify the rename.
   * <ol>
   *   <li>Values extracted from sourceStatus SHALL take priority over
   *       sourceEtag/sourceLastModified parameter.</li>
   *   <li>sourceStatus.getPath().getName() MUST equal source.getName()</li>
   *   <li>If store has a subclass of FileStatus and it is sourceStatus is of this type,
   *       custom information MAY be used to qualify/validate the request.
   *       This MAY include etag or S3 version ID extraction,</li>
   * </ol>
   *
   * Filesystems MAY support this call on an instance-by-instance basis, depending on
   * the nature of the remote store.
   * If not available the implementation MUST {@code ResilientCommitByRenameUnsupported}.
   * Callers SHOULD use a check of
   * {@code hasPathCapability(source, RESILIENT_COMMIT_BY_RENAME_PATH_CAPABILITY}
   * before trying to use this call.
   *
   * PostConditions on a successful operation:
   * <pre>
   * FS' where:
   *     not exists(FS', source)
   *     and exists(FS', dest)
   *     and data(FS', dest) == data (FS, source)
   * </pre>
   * This is exactly the same outcome as `FileSystem.rename()` when the same preconditions
   * are met. This API call simply restricts the operation to file rename with strict
   * conditions, (no need to be 'clever' about dest path calculation) and the ability
   * to pass in etags, modtimes and file status values.
   *
   * @param source path to source file
   * @param dest destination of rename.
   * @param sourceEtag etag of source file. may be null or empty
   * @param sourceStatus nullable FileStatus of source.
   * @throws FileNotFoundException source file not found
   * @throws ResilientCommitByRenameUnsupported not available on this store.
   * @throws PathIOException failure, including source and dest being the same path
   * @throws IOException any other exception
   */
  default CommitByRenameOutcome commitSingleFileByRename(
      Path source,
      Path dest,
      @Nullable String sourceEtag,
      @Nullable FileStatus sourceStatus)
      throws FileNotFoundException,
        ResilientCommitByRenameUnsupported,
        PathIOException,
        IOException {
    throw new ResilientCommitByRenameUnsupported(source.toString());
  }

  /**
   * The outcome. This is always a success, but it
   * may include some information about what happened.
   */
  class CommitByRenameOutcome implements IOStatisticsSource {

  }

  final class ResilientCommitByRenameUnsupported extends PathIOException {
    public ResilientCommitByRenameUnsupported(final String path) {
      super(path, "ResilientCommit operations not supported");
    }
  }
}
