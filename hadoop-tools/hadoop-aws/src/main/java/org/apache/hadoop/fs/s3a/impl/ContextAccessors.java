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

package org.apache.hadoop.fs.s3a.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;

import com.amazonaws.services.s3.model.ObjectMetadata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;

/**
 * An interface to implement for providing accessors to
 * S3AFileSystem-level API calls.
 * <p>
 * This is used to avoid giving any explicit reference to the owning
 * FS in the store context; there are enough calls that using lambda-expressions
 * gets over-complex.
 * <ol>
 *   <li>Test suites are free to provide their own implementation, using
 *  * the S3AFileSystem methods as the normative reference.</li>
 *  <li>All implementations <i>MUST</i> translate exceptions.</li>
 * </ol>
 */
public interface ContextAccessors {

  /**
   * Convert a key to a fully qualified path.
   * @param key input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  Path keyToPath(String key);

  /**
   * Turns a path (relative or otherwise) into an S3 key.
   *
   * @param path input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  String pathToKey(Path path);

  /**
   * Create a temporary file.
   * @param prefix prefix for the temporary file
   * @param size the size of the file that is going to be written
   * @return a unique temporary file
   * @throws IOException IO problems
   */
  File createTempFile(String prefix, long size) throws IOException;

  /**
   * Get the region of a bucket. This may be via an S3 API call if not
   * already cached.
   * @return the region in which a bucket is located
   * @throws AccessDeniedException if the caller lacks permission.
   * @throws IOException on any failure.
   */
  @Retries.RetryTranslated
  String getBucketLocation() throws IOException;

  /**
   * Qualify a path.
   *
   * @param path path to qualify/normalize
   * @return possibly new path.
   */
  Path makeQualified(Path path);

  /**
   * Retrieve the object metadata.
   *
   * @param key key to retrieve.
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @Retries.RetryTranslated
  ObjectMetadata getObjectMetadata(String key) throws IOException;

}
