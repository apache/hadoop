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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code MetadataStoreListFilesIterator} is a {@link RemoteIterator} that
 * is similar to {@code DescendantsIterator} but does not return directories
 * that have (or may have) children, and will also provide access to the set of
 * tombstones to allow recently deleted S3 objects to be filtered out from a
 * corresponding request.  In other words, it returns tombstones and the same
 * set of objects that should exist in S3: empty directories, and files, and not
 * other directories whose existence is inferred therefrom.
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
 * for (MetadataStoreListFilesIterator files =
 *     new MetadataStoreListFilesIterator(dir1); files.hasNext(); ) {
 *   final FileStatus status = files.next().getFileStatus();
 *   System.out.printf("%s %s%n", status.isDirectory() ? 'D' : 'F',
 *       status.getPath());
 * }
 * </pre>
 *
 * The output is:
 * <pre>
 * F /dir1/dir2/file1
 * F /dir1/dir2/file2
 * F /dir1/dir3/dir4/file3
 * F /dir1/dir3/dir5/file4
 * D /dir1/dir3/dir6
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetadataStoreListFilesIterator implements
    RemoteIterator<FileStatus> {
  public static final Logger LOG = LoggerFactory.getLogger(
      MetadataStoreListFilesIterator.class);

  private final boolean allowAuthoritative;
  private final MetadataStore metadataStore;
  private final Set<Path> tombstones = new HashSet<>();
  private Iterator<FileStatus> leafNodesIterator = null;

  public MetadataStoreListFilesIterator(MetadataStore ms, PathMetadata meta,
      boolean allowAuthoritative) throws IOException {
    Preconditions.checkNotNull(ms);
    this.metadataStore = ms;
    this.allowAuthoritative = allowAuthoritative;
    prefetch(meta);
  }

  private void prefetch(PathMetadata meta) throws IOException {
    final Queue<PathMetadata> queue = new LinkedList<>();
    final Collection<FileStatus> leafNodes = new ArrayList<>();

    if (meta != null) {
      final Path path = meta.getFileStatus().getPath();
      if (path.isRoot()) {
        DirListingMetadata rootListing = metadataStore.listChildren(path);
        if (rootListing != null) {
          tombstones.addAll(rootListing.listTombstones());
          queue.addAll(rootListing.withoutTombstones().getListing());
        }
      } else {
        queue.add(meta);
      }
    }

    while(!queue.isEmpty()) {
      PathMetadata nextMetadata = queue.poll();
      FileStatus nextStatus = nextMetadata.getFileStatus();
      if (nextStatus.isFile()) {
        // All files are leaf nodes by definition
        leafNodes.add(nextStatus);
        continue;
      }
      if (nextStatus.isDirectory()) {
        final Path path = nextStatus.getPath();
        DirListingMetadata children = metadataStore.listChildren(path);
        if (children != null) {
          tombstones.addAll(children.listTombstones());
          Collection<PathMetadata> liveChildren =
              children.withoutTombstones().getListing();
          if (!liveChildren.isEmpty()) {
            // If it's a directory, has children, not all deleted, then we
            // add the children to the queue and move on to the next node
            queue.addAll(liveChildren);
            continue;
          } else if (allowAuthoritative && children.isAuthoritative()) {
            leafNodes.add(nextStatus);
          }
        }
      }
      // Directories that *might* be empty are ignored for now, since we
      // cannot confirm that they are empty without incurring other costs.
      // Users of this class can still discover empty directories via S3's
      // fake directories, subject to the same consistency semantics as before.
      // The only other possibility is a symlink, which is unsupported on S3A.
    }
    leafNodesIterator = leafNodes.iterator();
  }

  @Override
  public boolean hasNext() {
    return leafNodesIterator.hasNext();
  }

  @Override
  public FileStatus next() {
    return leafNodesIterator.next();
  }

  public Set<Path> listTombstones() {
    return tombstones;
  }
}
