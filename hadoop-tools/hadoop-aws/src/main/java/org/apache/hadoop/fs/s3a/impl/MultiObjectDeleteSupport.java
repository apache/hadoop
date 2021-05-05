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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSS3IOException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

/**
 * Support for Multi Object Deletion.
 */
public final class MultiObjectDeleteSupport extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      MultiObjectDeleteSupport.class);

  private final BulkOperationState operationState;

  /**
   * Initiate with a store context.
   * @param context store context.
   * @param operationState any ongoing bulk operation.
   */
  public MultiObjectDeleteSupport(final StoreContext context,
      final BulkOperationState operationState) {
    super(context);
    this.operationState = operationState;
  }

  /**
   * This is the exception exit code if access was denied on a delete.
   * {@value}.
   */
  public static final String ACCESS_DENIED = "AccessDenied";

  /**
   * A {@code MultiObjectDeleteException} is raised if one or more
   * paths listed in a bulk DELETE operation failed.
   * The top-level exception is therefore just "something wasn't deleted",
   * but doesn't include the what or the why.
   * This translation will extract an AccessDeniedException if that's one of
   * the causes, otherwise grabs the status code and uses it in the
   * returned exception.
   * @param message text for the exception
   * @param deleteException the delete exception. to translate
   * @return an IOE with more detail.
   */
  public static IOException translateDeleteException(
      final String message,
      final MultiObjectDeleteException deleteException) {
    List<MultiObjectDeleteException.DeleteError> errors
        = deleteException.getErrors();
    LOG.warn("Bulk delete operation failed to delete all objects;"
            + " failure count = {}",
        errors.size());
    final StringBuilder result = new StringBuilder(
        errors.size() * 256);
    result.append(message).append(": ");
    String exitCode = "";
    for (MultiObjectDeleteException.DeleteError error :
        deleteException.getErrors()) {
      String code = error.getCode();
      String item = String.format("%s: %s%s: %s%n", code, error.getKey(),
          (error.getVersionId() != null
              ? (" (" + error.getVersionId() + ")")
              : ""),
          error.getMessage());
      LOG.warn(item);
      result.append(item);
      if (exitCode == null || exitCode.isEmpty() || ACCESS_DENIED.equals(code)) {
        exitCode = code;
      }
    }
    if (ACCESS_DENIED.equals(exitCode)) {
      return (IOException) new AccessDeniedException(result.toString())
          .initCause(deleteException);
    } else {
      return new AWSS3IOException(result.toString(), deleteException);
    }
  }

  /**
   * Process a multi object delete exception by building two paths from
   * the delete request: one of all deleted files, one of all undeleted values.
   * The latter are those rejected in the delete call.
   * @param deleteException the delete exception.
   * @param keysToDelete the keys in the delete request
   * @return tuple of (undeleted, deleted) paths.
   */
  public Pair<List<KeyPath>, List<KeyPath>> splitUndeletedKeys(
      final MultiObjectDeleteException deleteException,
      final Collection<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    LOG.debug("Processing delete failure; keys to delete count = {};"
            + " errors in exception {}; successful deletions = {}",
        keysToDelete.size(),
        deleteException.getErrors().size(),
        deleteException.getDeletedObjects().size());
    // convert the collection of keys being deleted into paths
    final List<KeyPath> pathsBeingDeleted = keysToKeyPaths(keysToDelete);
    // Take this ist of paths
    // extract all undeleted entries contained in the exception and
    // then remove them from the original list.
    List<KeyPath> undeleted = removeUndeletedPaths(deleteException,
        pathsBeingDeleted,
        getStoreContext()::keyToPath);
    return Pair.of(undeleted, pathsBeingDeleted);
  }

  /**
   * Given a list of delete requests, convert them all to paths.
   * @param keysToDelete list of keys for the delete operation.
   * @return the paths.
   */
  public List<Path> keysToPaths(
      final Collection<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    return toPathList(keysToKeyPaths(keysToDelete));
  }

  /**
   * Given a list of delete requests, convert them all to keypaths.
   * @param keysToDelete list of keys for the delete operation.
   * @return list of keypath entries
   */
  public List<KeyPath> keysToKeyPaths(
      final Collection<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    return convertToKeyPaths(keysToDelete,
        getStoreContext()::keyToPath);
  }

  /**
   * Given a list of delete requests, convert them all to paths.
   * @param keysToDelete list of keys for the delete operation.
   * @param qualifier path qualifier
   * @return the paths.
   */
  public static List<KeyPath> convertToKeyPaths(
      final Collection<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final Function<String, Path> qualifier) {
    List<KeyPath> l = new ArrayList<>(keysToDelete.size());
    for (DeleteObjectsRequest.KeyVersion kv : keysToDelete) {
      String key = kv.getKey();
      Path p = qualifier.apply(key);
      boolean isDir = key.endsWith("/");
      l.add(new KeyPath(key, p, isDir));
    }
    return l;
  }

  /**
   * Process a delete failure by removing from the metastore all entries
   * which where deleted, as inferred from the delete failures exception
   * and the original list of files to delete declares to have been deleted.
   * @param deleteException the delete exception.
   * @param keysToDelete collection of keys which had been requested.
   * @param retainedMarkers list built up of retained markers.
   * @return a tuple of (undeleted, deleted, failures)
   */
  public Triple<List<Path>, List<Path>, List<Pair<Path, IOException>>>
      processDeleteFailure(
      final MultiObjectDeleteException deleteException,
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final List<Path> retainedMarkers) {
    final MetadataStore metadataStore =
        checkNotNull(getStoreContext().getMetadataStore(),
            "context metadatastore");
    final List<Pair<Path, IOException>> failures = new ArrayList<>();
    final Pair<List<KeyPath>, List<KeyPath>> outcome =
        splitUndeletedKeys(deleteException, keysToDelete);
    List<KeyPath> deleted = outcome.getRight();
    List<Path> deletedPaths = new ArrayList<>();
    List<KeyPath> undeleted = outcome.getLeft();
    retainedMarkers.clear();
    List<Path> undeletedPaths = toPathList((List<KeyPath>) undeleted);
    // sort shorter keys first,
    // so that if the left key is longer than the first it is considered
    // smaller, so appears in the list first.
    // thus when we look for a dir being empty, we know it holds
    deleted.sort((l, r) -> r.getKey().length() - l.getKey().length());

    // now go through and delete from S3Guard all paths listed in
    // the result which are either files or directories with
    // no children.
    deleted.forEach(kp -> {
      Path path = kp.getPath();
      try{
        boolean toDelete = true;
        if (kp.isDirectoryMarker()) {
          // its a dir marker, which could be an empty dir
          // (which is then tombstoned), or a non-empty dir, which
          // is not tombstoned.
          // for this to be handled, we have to have removed children
          // from the store first, which relies on the sort
          PathMetadata pmentry = metadataStore.get(path, true);
          if (pmentry != null && !pmentry.isDeleted()) {
            toDelete = pmentry.getFileStatus().isEmptyDirectory()
                == Tristate.TRUE;
          } else {
            toDelete = false;
          }
        }
        if (toDelete) {
          LOG.debug("Removing deleted object from S3Guard Store {}", path);
          metadataStore.delete(path, operationState);
        } else {
          LOG.debug("Retaining S3Guard directory entry {}", path);
          retainedMarkers.add(path);
        }
      } catch (IOException e) {
        // trouble: we failed to delete the far end entry
        // try with the next one.
        // if this is a big network failure, this is going to be noisy.
        LOG.warn("Failed to update S3Guard store with deletion of {}", path);
        failures.add(Pair.of(path, e));
      }
      // irrespective of the S3Guard outcome, it is declared as deleted, as
      // it is no longer in the S3 store.
      deletedPaths.add(path);
    });
    if (LOG.isDebugEnabled()) {
      undeleted.forEach(p -> LOG.debug("Deleted {}", p));
    }
    return Triple.of(undeletedPaths, deletedPaths, failures);
  }

  /**
   * Given a list of keypaths, convert to a list of paths.
   * @param keyPaths source list
   * @return a listg of paths
   */
  public static List<Path> toPathList(final List<KeyPath> keyPaths) {
    return keyPaths.stream()
        .map(KeyPath::getPath)
        .collect(Collectors.toList());
  }

  /**
   * Build a list of undeleted paths from a {@code MultiObjectDeleteException}.
   * Outside of unit tests, the qualifier function should be
   * {@link S3AFileSystem#keyToQualifiedPath(String)}.
   * @param deleteException the delete exception.
   * @param qualifierFn function to qualify paths
   * @return the possibly empty list of paths.
   */
  @VisibleForTesting
  public static List<Path> extractUndeletedPaths(
      final MultiObjectDeleteException deleteException,
      final Function<String, Path> qualifierFn) {
    return toPathList(extractUndeletedKeyPaths(deleteException, qualifierFn));
  }

  /**
   * Build a list of undeleted paths from a {@code MultiObjectDeleteException}.
   * Outside of unit tests, the qualifier function should be
   * {@link S3AFileSystem#keyToQualifiedPath(String)}.
   * @param deleteException the delete exception.
   * @param qualifierFn function to qualify paths
   * @return the possibly empty list of paths.
   */
  @VisibleForTesting
  public static List<KeyPath> extractUndeletedKeyPaths(
      final MultiObjectDeleteException deleteException,
      final Function<String, Path> qualifierFn) {

    List<MultiObjectDeleteException.DeleteError> errors
        = deleteException.getErrors();
    return errors.stream()
        .map((error) -> {
          String key = error.getKey();
          Path path = qualifierFn.apply(key);
          boolean isDir = key.endsWith("/");
          return new KeyPath(key, path, isDir);
        })
        .collect(Collectors.toList());
  }

  /**
   * Process a {@code MultiObjectDeleteException} by
   * removing all undeleted paths from the list of paths being deleted.
   * The original list is updated, and so becomes the list of successfully
   * deleted paths.
   * @param deleteException the delete exception.
   * @param pathsBeingDeleted list of paths which were being deleted.
   * This has all undeleted paths removed, leaving only those deleted.
   * @return the list of undeleted entries
   */
  @VisibleForTesting
  static List<KeyPath> removeUndeletedPaths(
      final MultiObjectDeleteException deleteException,
      final Collection<KeyPath> pathsBeingDeleted,
      final Function<String, Path> qualifier) {
    // get the undeleted values
    List<KeyPath> undeleted = extractUndeletedKeyPaths(deleteException,
        qualifier);
    // and remove them from the undeleted list, matching on key
    for (KeyPath undel : undeleted) {
      pathsBeingDeleted.removeIf(kp -> kp.getPath().equals(undel.getPath()));
    }
    return undeleted;
  }

  /**
   * A delete operation failed.
   * Currently just returns the list of all paths.
   * @param ex exception.
   * @param keysToDelete the keys which were being deleted.
   * @return all paths which were not deleted.
   */
  public List<Path> processDeleteFailureGenericException(Exception ex,
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    return keysToPaths(keysToDelete);
  }

  /**
   * Representation of a (key, path) which couldn't be deleted;
   * the dir marker flag is inferred from the key suffix.
   * <p>
   * Added because Pairs of Lists of Triples was just too complex
   * for Java code.
   * </p>
   */
  public static final class KeyPath {
    /** Key in bucket. */
    private final String key;
    /** Full path. */
    private final Path path;
    /** Is this a directory marker? */
    private final boolean directoryMarker;

    public KeyPath(final String key,
        final Path path,
        final boolean directoryMarker) {
      this.key = key;
      this.path = path;
      this.directoryMarker = directoryMarker;
    }

    public String getKey() {
      return key;
    }

    public Path getPath() {
      return path;
    }

    public boolean isDirectoryMarker() {
      return directoryMarker;
    }

    @Override
    public String toString() {
      return "KeyPath{" +
          "key='" + key + '\'' +
          ", path=" + path +
          ", directoryMarker=" + directoryMarker +
          '}';
    }

    /**
     * Equals test is on key alone.
     */
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyPath keyPath = (KeyPath) o;
      return key.equals(keyPath.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key);
    }
  }
}
