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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * API exclusively for committing files.
 *
 * This is only for use by (@link {@link AbfsManifestStoreOperations},
 * and is intended to be implemented by ABFS.
 * To ensure that there is no need to add mapreduce JARs to the
 * classpath just to work with ABFS, this interface
 * MUST NOT refer to anything in the
 * {@code org.apache.hadoop.mapreduce} package.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ResilientCommitByRename extends IOStatisticsSource {

  /**
   * Rename source file to dest path *Exactly*; no subdirectory games here.
   * if the method does not raise an exception,then
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
   * Post Conditions on a successful operation:
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
   * @return true if recovery was needed.
   * @throws FileNotFoundException source file not found
   * @throws PathIOException failure, including source and dest being the same path
   * @throws IOException any other exception
   */
  Pair<Boolean, Duration> commitSingleFileByRename(
      Path source,
      Path dest,
      @Nullable String sourceEtag) throws IOException;


}
