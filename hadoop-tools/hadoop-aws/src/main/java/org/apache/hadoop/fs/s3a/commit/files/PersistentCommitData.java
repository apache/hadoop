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

package org.apache.hadoop.fs.s3a.commit.files;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.ValidationFailure;

/**
 * Class for single/multiple commit data structures.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PersistentCommitData implements Serializable {

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@code serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 1;

  /**
   * Validate the data: those fields which must be non empty, must be set.
   * @throws ValidationFailure if the data is invalid
   */
  public abstract void validate() throws ValidationFailure;

  /**
   * Serialize to JSON and then to a byte array, after performing a
   * preflight validation of the data to be saved.
   * @return the data in a persistable form.
   * @throws IOException serialization problem or validation failure.
   */
  public abstract byte[] toBytes() throws IOException;

  /**
   * Save to a hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public abstract void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException;

}
