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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

/**
 * {@code DescendantsIterator} is a {@link RemoteIterator} that implements
 * pre-ordering breadth-first traversal (BFS) of a path and all of its
 * descendants recursively.  After visiting each path, that path's direct
 * children are discovered by calling {@link MetadataStore#listChildren(Path)}.
 * Each iteration returns the next direct child, and if that child is a
 * directory, also pushes it onto a queue to discover its children later.
 *
 * For example, assume the consistent store contains metadata representing this
 * file system structure:
 *
 * <pre>
 * /dir1
 * |-- dir2
 * |   |-- file1
 * |   `-- file2
 * `-- dir3
 *     |-- dir4
 *     |   `-- file3
 *     |-- dir5
 *     |   `-- file4
 *     `-- dir6
 * </pre>
 *
 * Consider this code sample:
 * <pre>
 * final PathMetadata dir1 = get(new Path("/dir1"));
 * for (DescendantsIterator descendants = new DescendantsIterator(dir1);
 *     descendants.hasNext(); ) {
 *   final FileStatus status = descendants.next().getFileStatus();
 *   System.out.printf("%s %s%n", status.isDirectory() ? 'D' : 'F',
 *       status.getPath());
 * }
 * </pre>
 *
 * The output is:
 * <pre>
 * D /dir1
 * D /dir1/dir2
 * D /dir1/dir3
 * F /dir1/dir2/file1
 * F /dir1/dir2/file2
 * D /dir1/dir3/dir4
 * D /dir1/dir3/dir5
 * F /dir1/dir3/dir4/file3
 * F /dir1/dir3/dir5/file4
 * D /dir1/dir3/dir6
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DescendantsIterator implements RemoteIterator<S3AFileStatus> {

  private final MetadataStore metadataStore;
  private final Queue<PathMetadata> queue = new LinkedList<>();

  /**
   * Creates a new {@code DescendantsIterator}.
   *
   * @param ms the associated {@link MetadataStore}
   * @param meta base path for descendants iteration, which will be the first
   *     returned during iteration (except root). Null makes empty iterator.
   * @throws IOException if errors happen during metadata store listing
   */
  public DescendantsIterator(MetadataStore ms, PathMetadata meta)
      throws IOException {
    Preconditions.checkNotNull(ms);
    this.metadataStore = ms;

    if (meta != null) {
      final Path path = meta.getFileStatus().getPath();
      if (path.isRoot()) {
        DirListingMetadata rootListing = ms.listChildren(path);
        if (rootListing != null) {
          rootListing = rootListing.withoutTombstones();
          queue.addAll(rootListing.getListing());
        }
      } else {
        queue.add(meta);
      }
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    return !queue.isEmpty();
  }

  @Override
  public S3AFileStatus next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException("No more descendants.");
    }
    PathMetadata next;
    next = queue.poll();
    if (next.getFileStatus().isDirectory()) {
      final Path path = next.getFileStatus().getPath();
      DirListingMetadata meta = metadataStore.listChildren(path);
      if (meta != null) {
        Collection<PathMetadata> more = meta.withoutTombstones().getListing();
        if (!more.isEmpty()) {
          queue.addAll(more);
        }
      }
    }
    return next.getFileStatus();
  }
}
