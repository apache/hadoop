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

package org.apache.hadoop.fs.shell;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.util.DurationInfo;

/**
 * Trash policy which deletes files, logging how long it takes.
 * It still "claims" to be enabled via {@link #isEnabled()} but
 * it isn't.
 */
public class DeleteFilesTrashPolicy extends TrashPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteFilesTrashPolicy.class);

  private static final Path CURRENT = new Path("Current");

  private FileSystem filesystem;

  @Override
  public void initialize(final Configuration conf,
      final FileSystem fs,
      final Path home) {
    initialize(conf, fs);
  }

  @Override
  public void initialize(final Configuration conf, final FileSystem fs) {
    filesystem = fs;
  }

  @Override
  public Path getCurrentTrashDir(final Path path) throws IOException {
    return new Path(fs.getTrashRoot(path), CURRENT);
  }


  @Override
  public Path getCurrentTrashDir() {
    return new Path(fs.getTrashRoot(null), CURRENT);
  }


  @Override
  public boolean isEnabled() {
    return true;
  }

  /**
   * Delete, logging duration.
   * @param path the path.
   * @return
   * @throws IOException
   */
  @Override
  public boolean moveToTrash(final Path path) throws IOException {
    try (DurationInfo info = new DurationInfo(LOG, true, "delete %s", path)) {
      return filesystem.delete(path, true);
    }
  }

  @Override
  public void createCheckpoint() throws IOException {

  }

  @Override
  public void deleteCheckpoint() throws IOException {

  }

  @Override
  public void deleteCheckpointsImmediately() throws IOException {

  }

  /**
   * Return a no-op {@link Runnable}.
   *
   * @return An empty Runnable.
   */
  @Override
  public Runnable getEmptier() throws IOException {
    return () -> {};
  }

}
