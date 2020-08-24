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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.util.List;

/**
 * Interface filesystems MAY implement to offer a batched operations.
 */

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BatchRename {

  /**
   * Batched rename API that rename a batch of files.
   *
   * @param srcs source file list.
   * @param dsts target file list.
   * @throws IOException failure exception.
   */
  void batchRename(List<String> srcs, List<String> dsts,
      Options.Rename... options) throws IOException;
}
