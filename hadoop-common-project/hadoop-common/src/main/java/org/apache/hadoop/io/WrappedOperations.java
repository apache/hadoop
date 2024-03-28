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

package org.apache.hadoop.io;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.util.functional.FutureIO;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;

/**
 * Reflection-friendly access to APIs which are not available in
 * some of the older Hadoop versions which libraries still
 * compile against.
 * <p>
 * The intent is to avoid the need for complex reflection operations
 * including wrapping of parameter classes, direct instatiation of
 * new classes etc.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class WrappedOperations {

  private WrappedOperations() {
  }

  /**
   * OpenFile assistant, easy reflection-based access to
   * {@link FileSystem#openFile(org.apache.hadoop.fs.Path)}.
   * @param fs filesystem
   * @param path path
   * @param policy read policy
   * @param status optional file status
   * @param options nullable map of other options
   * @return an opened file
   * @throws IOException for any failure.
   */
  public static FSDataInputStream openFile(final FileSystem fs,
      final org.apache.hadoop.fs.Path path,
      final String policy,
      final FileStatus status,
      final Map<String, String> options) throws IOException {
    final FutureDataInputStreamBuilder builder = fs.openFile(path);
    if (policy != null) {
      builder.opt(FS_OPTION_OPENFILE_READ_POLICY, policy);
    }
    if (status != null) {
      builder.withFileStatus(status);
    }
    if (options != null) {
      // add all the options map entries
      options.forEach(builder::opt);
    }
    // wait for the opening.
    return FutureIO.awaitFuture(builder.build());
  }
}
