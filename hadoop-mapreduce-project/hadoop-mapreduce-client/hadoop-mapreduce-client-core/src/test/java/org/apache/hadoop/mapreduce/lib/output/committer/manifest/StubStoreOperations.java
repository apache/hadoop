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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.util.JsonSerialization;

/**
 * Stub Store operations.
 * Everything "works" provided you don't look too close.
 * Files have etags of their filename; if you move a file without changing its
 * name, the etag is preserved.
 */
public class StubStoreOperations extends ManifestStoreOperations {

  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    return new TaggedFileStatus(0, false, 1, 1024, 0, path, path.getName());
  }

  @Override
  public boolean delete(final Path path, final boolean recursive)
      throws IOException {
    return true;
  }

  @Override
  public boolean mkdirs(final Path path) throws IOException {
    return true;
  }

  @Override
  public boolean renameFile(final Path source, final Path dest)
      throws IOException {
    return true;
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path path)
      throws IOException {
    return new EmptyRemoteIterator<>();
  }

  @Override
  public TaskManifest loadTaskManifest(JsonSerialization<TaskManifest> serializer,
      final FileStatus st) throws IOException {
    return new TaskManifest();
  }

  @Override
  public <T extends AbstractManifestData<T>> void save(T manifestData,
      final Path path,
      final boolean overwrite) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean storePreservesEtagsThroughRenames(final Path path) {
    return true;
  }

  /**
   * Always empty rempte iterator.
   * @param <T> type of iterator.
   */
  private static final class EmptyRemoteIterator<T>
      implements RemoteIterator<T> {

    @Override
    public boolean hasNext() throws IOException {
      return false;
    }

    @Override
    public T next() throws IOException {
      throw new NoSuchElementException();
    }
  }

}
