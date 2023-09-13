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

package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * A No-Op Trash Policy for Filesystems which prefers to have no trash
 * enabled for them.
 */
public class EmptyTrashPolicy extends TrashPolicy {
  @Override
  public void initialize(Configuration conf, FileSystem fs, Path home) {

  }

  public void initialize(Configuration conf, FileSystem fs) {

  }

  @Override public boolean isEnabled() {
    return false;
  }

  @Override public boolean moveToTrash(Path path) throws IOException {
    return false;
  }

  @Override public void createCheckpoint() throws IOException {

  }

  @Override public void deleteCheckpoint() throws IOException {

  }

  @Override public void deleteCheckpointsImmediately() throws IOException {

  }

  @Override public Path getCurrentTrashDir() {
    return null;
  }

  @Override public Runnable getEmptier() throws IOException {
    return null;
  }
}
