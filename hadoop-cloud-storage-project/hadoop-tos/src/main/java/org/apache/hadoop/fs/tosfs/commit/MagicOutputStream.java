/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.commit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.object.ObjectOutputStream;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class MagicOutputStream extends ObjectOutputStream {

  private final FileSystem fs;
  private final Path pendingPath;
  private boolean closeStorage = false;

  public MagicOutputStream(FileSystem fs, ExecutorService threadPool, Configuration conf,
      Path magic) {
    this(fs,
        ObjectStorageFactory.create(magic.toUri().getScheme(), magic.toUri().getHost(), conf),
        threadPool,
        conf,
        magic);
    closeStorage = true;
  }

  public MagicOutputStream(FileSystem fs, ObjectStorage storage, ExecutorService threadPool,
      Configuration conf, Path magic) {
    super(storage, threadPool, conf, magic, false);
    this.fs = fs;
    this.pendingPath = createPendingPath(magic);
  }

  static String toDestKey(Path magicPath) {
    Preconditions.checkArgument(isMagic(magicPath), "Destination path is not magic %s", magicPath);
    String magicKey = ObjectUtils.pathToKey(magicPath);
    List<String> splits = Lists.newArrayList(magicKey.split("/"));

    // Break the full splits list into three collections: <parentSplits>, __magic, <childrenSplits>
    int magicIndex = splits.indexOf(CommitUtils.MAGIC);
    Preconditions.checkArgument(magicIndex >= 0, "Cannot locate %s in path %s", CommitUtils.MAGIC,
        magicPath);
    List<String> parentSplits = splits.subList(0, magicIndex);
    List<String> childrenSplits = splits.subList(magicIndex + 1, splits.size());
    Preconditions.checkArgument(!childrenSplits.isEmpty(),
        "No path found under %s for path %s", CommitUtils.MAGIC, magicPath);

    // Generate the destination splits which will be joined into the destination object key.
    List<String> destSplits = Lists.newArrayList(parentSplits);
    if (childrenSplits.contains(CommitUtils.BASE)) {
      // Break the <childrenDir> into three collections: <baseParentSplits>, __base,
      // <baseChildrenSplits>, and add all <baseChildrenSplits> into the destination splits.
      int baseIndex = childrenSplits.indexOf(CommitUtils.BASE);
      Preconditions.checkArgument(baseIndex >= 0, "Cannot locate %s in path %s", CommitUtils.BASE,
          magicPath);
      List<String> baseChildrenSplits =
          childrenSplits.subList(baseIndex + 1, childrenSplits.size());
      Preconditions.checkArgument(!baseChildrenSplits.isEmpty(),
          "No path found under %s for magic path %s", CommitUtils.BASE, magicPath);
      destSplits.addAll(baseChildrenSplits);
    } else {
      // Just add the last elements of the <childrenSplits> into the destination splits.
      String filename = childrenSplits.get(childrenSplits.size() - 1);
      destSplits.add(filename);
    }

    return StringUtils.join(destSplits, "/");
  }

  @Override
  protected String createDestKey(Path magicPath) {
    return toDestKey(magicPath);
  }

  @Override
  protected void finishUpload(String destKey, String uploadId, List<Part> parts)
      throws IOException {
    Pending pending = Pending.builder()
        .setBucket(storage().bucket().name())
        .setUploadId(uploadId)
        .setLength(parts.stream().mapToLong(Part::size).sum())
        .setDestKey(destKey)
        .setCreatedTimestamp(System.currentTimeMillis())
        .addParts(parts)
        .build();

    persist(pendingPath, pending.serialize());
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (closeStorage) {
      storage().close();
    }
  }

  protected void persist(Path p, byte[] data) throws IOException {
    CommitUtils.save(fs, p, data);
  }

  public String pendingKey() {
    return ObjectUtils.pathToKey(pendingPath);
  }

  private static Path createPendingPath(Path magic) {
    return new Path(magic.getParent(),
        String.format("%s%s", magic.getName(), CommitUtils.PENDING_SUFFIX));
  }

  // .pending and .pendingset files are not typical magic files.
  private static boolean isInternalFile(Path p) {
    return p.toString().endsWith(CommitUtils.PENDINGSET_SUFFIX) || p.toString()
        .endsWith(CommitUtils.PENDING_SUFFIX);
  }

  public static boolean isMagic(Path p) {
    Preconditions.checkNotNull(p, "path cannot be null.");
    String path = p.toUri().getPath();
    List<String> splits = Arrays.stream(path.split("/"))
        .filter(StringUtils::isNoneEmpty)
        .collect(Collectors.toList());
    return splits.contains(CommitUtils.MAGIC) && !isInternalFile(p);
  }
}
