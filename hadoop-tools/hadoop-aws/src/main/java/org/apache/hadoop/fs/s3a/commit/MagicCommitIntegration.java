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

package org.apache.hadoop.fs.s3a.commit;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTracker;

import static org.apache.hadoop.fs.s3a.commit.MagicCommitPaths.*;

/**
 * Adds the code needed for S3A to support magic committers.
 * It's pulled out to keep S3A FS class slightly less complex.
 * This class can be instantiated even when magic commit is disabled;
 * in this case:
 * <ol>
 *   <li>{@link #isMagicCommitPath(Path)} will always return false.</li>
 *   <li>{@link #createTracker(Path, String)} will always return an instance
 *   of {@link PutTracker}.</li>
 * </ol>
 *
 * <p>Important</p>: must not directly or indirectly import a class which
 * uses any datatype in hadoop-mapreduce.
 */
public class MagicCommitIntegration {
  private static final Logger LOG =
      LoggerFactory.getLogger(MagicCommitIntegration.class);
  private final S3AFileSystem owner;
  private final boolean magicCommitEnabled;

  /**
   * Instantiate.
   * @param owner owner class
   * @param magicCommitEnabled is magic commit enabled.
   */
  public MagicCommitIntegration(S3AFileSystem owner,
      boolean magicCommitEnabled) {
    this.owner = owner;
    this.magicCommitEnabled = magicCommitEnabled;
  }

  /**
   * Given an (elements, key) pair, return the key of the final destination of
   * the PUT, that is: where the final path is expected to go?
   * @param elements path split to elements
   * @param key key
   * @return key for final put. If this is not a magic commit, the
   * same as the key in.
   */
  public String keyOfFinalDestination(List<String> elements, String key) {
    if (isMagicCommitPath(elements)) {
      return elementsToKey(finalDestination(elements));
    } else {
      return key;
    }
  }

  /**
   * Given a path and a key to that same path, create a tracker for it.
   * This specific tracker will be chosen based on whether or not
   * the path is a magic one.
   * @param path path of nominal write
   * @param key key of path of nominal write
   * @return the tracker for this operation.
   */
  public PutTracker createTracker(Path path, String key) {
    final List<String> elements = splitPathToElements(path);
    PutTracker tracker;

    if(isMagicFile(elements)) {
      // path is of a magic file
      if (isMagicCommitPath(elements)) {
        final String destKey = keyOfFinalDestination(elements, key);
        String pendingsetPath = key + CommitConstants.PENDING_SUFFIX;
        owner.getInstrumentation()
            .incrementCounter(Statistic.COMMITTER_MAGIC_FILES_CREATED, 1);
        tracker = new MagicCommitTracker(path,
            owner.getBucket(),
            key,
            destKey,
            pendingsetPath,
            owner.createWriteOperationHelper());
        LOG.debug("Created {}", tracker);
      } else {
        LOG.warn("File being created has a \"magic\" path, but the filesystem"
            + " has magic file support disabled: {}", path);
        // downgrade to standard multipart tracking
        tracker = new PutTracker(key);
      }
    } else {
      // standard multipart tracking
      tracker = new PutTracker(key);
    }
    return tracker;
  }

  /**
   * This performs the calculation of the final destination of a set
   * of elements.
   *
   * @param elements original (do not edit after this call)
   * @return a list of elements, possibly empty
   */
  private List<String> finalDestination(List<String> elements) {
    return magicCommitEnabled ?
        MagicCommitPaths.finalDestination(elements)
        : elements;
  }

  /**
   * Is magic commit enabled?
   * @return true if magic commit is turned on.
   */
  public boolean isMagicCommitEnabled() {
    return magicCommitEnabled;
  }

  /**
   * Predicate: is a path a magic commit path?
   * @param path path to examine
   * @return true if the path is or is under a magic directory
   */
  public boolean isMagicCommitPath(Path path) {
    return isMagicCommitPath(splitPathToElements(path));
  }

  /**
   * Is this path a magic commit path in this filesystem?
   * True if magic commit is enabled, the path is magic
   * and the path is not actually a commit metadata file.
   * @param elements element list
   * @return true if writing path is to be uprated to a magic file write
   */
  private boolean isMagicCommitPath(List<String> elements) {
    return magicCommitEnabled && isMagicFile(elements);
  }

  /**
   * Is the file a magic file: this predicate doesn't check
   * for the FS actually having the magic bit being set.
   * @param elements path elements
   * @return true if the path is one a magic file write expects.
   */
  private boolean isMagicFile(List<String> elements) {
    return isMagicPath(elements) &&
        !isCommitMetadataFile(elements);
  }

  /**
   * Does this file contain all the commit metadata?
   * @param elements path element list
   * @return true if this file is one of the commit metadata files.
   */
  private boolean isCommitMetadataFile(List<String> elements) {
    String last = elements.get(elements.size() - 1);
    return last.endsWith(CommitConstants.PENDING_SUFFIX)
        || last.endsWith(CommitConstants.PENDINGSET_SUFFIX);
  }

}
