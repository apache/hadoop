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
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSS3IOException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Support for Multi Object Deletion.
 */
public final class MultiObjectDeleteSupport {

  private static final Logger LOG = LoggerFactory.getLogger(
      MultiObjectDeleteSupport.class);

  private final StoreContext context;

  public MultiObjectDeleteSupport(final StoreContext context) {
    this.context = context;
  }

  /**
   * This is the exception exit code if access was denied on a delete.
   * {@value}.
   */
  public static final String ACCESS_DENIED = "AccessDenied";

  /**
   * A {@code }MultiObjectDeleteException} is raised if one or more
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
  public static IOException translateMultiObjectDeleteException(
      final String message,
      final MultiObjectDeleteException deleteException) {
    final StringBuilder result = new StringBuilder(
        deleteException.getErrors().size() * 256);
    result.append(message).append(": ");
    String exitCode = "";
    for (MultiObjectDeleteException.DeleteError error :
        deleteException.getErrors()) {
      String code = error.getCode();
      result.append(String.format("%s: %s: %s%n", code, error.getKey(),
          error.getMessage()));
      if (exitCode.isEmpty() || ACCESS_DENIED.equals(code)) {
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
   * Build a list of undeleted paths from a {@code MultiObjectDeleteException}.
   * Outside of unit tests, the qualifier function should be
   * {@link S3AFileSystem#keyToQualifiedPath(String)}.
   * @param deleteException the delete exception.
   * @param qualifierFn function to qualify paths
   * @return the possibly empty list of paths.
   */
  public static List<Path> extractUndeletedPaths(
      final MultiObjectDeleteException deleteException,
      final Function<String, Path> qualifierFn) {
    return deleteException.getErrors().stream()
        .map((e) -> qualifierFn.apply(e.getKey()))
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
  public static List<Path> removeUndeletedPaths(
      final MultiObjectDeleteException deleteException,
      final Collection<Path> pathsBeingDeleted,
      final Function<String, Path> qualifier) {
    List<Path> undeleted = extractUndeletedPaths(deleteException, qualifier);
    pathsBeingDeleted.removeAll(undeleted);
    return undeleted;
  }

  /**
   * Process a multi object delete exception by building two paths from
   * the delete request: one of all deleted files, one of all undeleted values.
   * The latter are those rejected in the delete call.
   * @param deleteException the delete exception.
   * @param keysToDelete the keys in the delete request
   * @return tuple of (undeleted, deleted) paths.
   */
  public Pair<List<Path>, List<Path>> splitUndeletedKeys(
      final MultiObjectDeleteException deleteException,
      final Collection<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    LOG.debug("Processing delete failure; keys to delete count = {};"
            + " errors in exception {}; successful deletions = {}",
        keysToDelete.size(),
        deleteException.getErrors().size(),
        deleteException.getDeletedObjects().size());
    Function<String, Path> qualifier = context.getKeyToPathQualifier();
    // convert the collection of keys being deleted into paths
    final List<Path> pathsBeingDeleted = keysToDelete.stream()
        .map((keyVersion) -> qualifier.apply(keyVersion.getKey()))
        .collect(Collectors.toList());
    // Take this is list of paths
    // extract all undeleted entries contained in the exception and
    // then removes them from the original list.
    List<Path> undeleted = removeUndeletedPaths(deleteException, pathsBeingDeleted,
        qualifier);
    return Pair.of(undeleted, pathsBeingDeleted);
  }

  /**
   * Process a delete failure by removing from the metastore all entries
   * which where deleted, as inferred from the delete failures exception
   * and the original list of files to delete declares to have been delted.
   * @param deleteException the delete exception.
   * @param keysToDelete collection of keys which had been requested.
   * @param qualifierFn qualifier to convert keys to paths
   * @return a tuple of (undeleted, deleted, failures)
   */
  public Triple<List<Path>, List<Path>, List<Pair<Path, IOException>>>
  processDeleteFailure(
      final MultiObjectDeleteException deleteException,
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    final MetadataStore metadataStore =
        checkNotNull(context.getMetadataStore(), "context metadatastore");
    final List<Pair<Path, IOException>> failures = new ArrayList<>();
    final Pair<List<Path>, List<Path>> outcome = splitUndeletedKeys(
        deleteException, keysToDelete);
    List<Path> deleted = outcome.getRight();
    List<Path> undeleted = outcome.getLeft();
    // delete the paths but recover
    deleted.forEach(path -> {
      try {
        metadataStore.delete(path);
      } catch (IOException e) {
        // trouble: we failed to delete the far end entry
        // try with the next one.
        // if this is a big network failure, this is going to be noisy.
        LOG.warn("Failed to update S3Guard store with deletion of {}", path);
        failures.add(Pair.of(path, e));
      }
    });
    if (LOG.isDebugEnabled()) {
      undeleted.forEach(p -> LOG.debug("Deleted {}", p));
    }
    return Triple.of(undeleted, deleted, failures);
  }

}
