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

package org.apache.hadoop.fs.obs;

import com.obs.services.exception.ObsException;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * OBS listing implementation.
 */
class OBSListing {
  /**
   * A Path filter which accepts all filenames.
   */
  static final PathFilter ACCEPT_ALL =
      new PathFilter() {
        @Override
        public boolean accept(final Path file) {
          return true;
        }

        @Override
        public String toString() {
          return "ACCEPT_ALL";
        }
      };

  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(OBSListing.class);

  /**
   * OBS File System instance.
   */
  private final OBSFileSystem owner;

  OBSListing(final OBSFileSystem ownerFS) {
    this.owner = ownerFS;
  }

  /**
   * Create a FileStatus iterator against a path, with a given list object
   * request.
   *
   * @param listPath path of the listing
   * @param request  initial request to make
   * @param filter   the filter on which paths to accept
   * @param acceptor the class/predicate to decide which entries to accept in
   *                 the listing based on the full file status.
   * @return the iterator
   * @throws IOException IO Problems
   */
  FileStatusListingIterator createFileStatusListingIterator(
      final Path listPath,
      final ListObjectsRequest request,
      final PathFilter filter,
      final FileStatusAcceptor acceptor)
      throws IOException {
    return new FileStatusListingIterator(
        new ObjectListingIterator(listPath, request), filter, acceptor);
  }

  /**
   * Create a located status iterator over a file status iterator.
   *
   * @param statusIterator an iterator over the remote status entries
   * @return a new remote iterator
   */
  LocatedFileStatusIterator createLocatedFileStatusIterator(
      final RemoteIterator<FileStatus> statusIterator) {
    return new LocatedFileStatusIterator(statusIterator);
  }

  /**
   * Interface to implement by the logic deciding whether to accept a summary
   * entry or path as a valid file or directory.
   */
  interface FileStatusAcceptor {

    /**
     * Predicate to decide whether or not to accept a summary entry.
     *
     * @param keyPath qualified path to the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry should be
     * generated.
     */
    boolean accept(Path keyPath, ObsObject summary);

    /**
     * Predicate to decide whether or not to accept a prefix.
     *
     * @param keyPath      qualified path to the entry
     * @param commonPrefix the prefix
     * @return true if the entry is accepted (i.e. that a status entry should be
     * generated.)
     */
    boolean accept(Path keyPath, String commonPrefix);
  }

  /**
   * A remote iterator which only iterates over a single `LocatedFileStatus`
   * value.
   *
   * <p>If the status value is null, the iterator declares that it has no
   * data. This iterator is used to handle
   * {@link OBSFileSystem#listStatus(Path)}calls where the path handed in
   * refers to a file, not a directory: this is
   * the iterator returned.
   */
  static final class SingleStatusRemoteIterator
      implements RemoteIterator<LocatedFileStatus> {

    /**
     * The status to return; set to null after the first iteration.
     */
    private LocatedFileStatus status;

    /**
     * Constructor.
     *
     * @param locatedFileStatus status value: may be null, in which case the
     *                          iterator is empty.
     */
    SingleStatusRemoteIterator(final LocatedFileStatus locatedFileStatus) {
      this.status = locatedFileStatus;
    }

    /**
     * {@inheritDoc}
     *
     * @return true if there is a file status to return: this is always false
     * for the second iteration, and may be false for the first.
     */
    @Override
    public boolean hasNext() {
      return status != null;
    }

    /**
     * {@inheritDoc}
     *
     * @return the non-null status element passed in when the instance was
     * constructed, if it ha not already been retrieved.
     * @throws NoSuchElementException if this is the second call, or it is the
     *                                first call and a null
     *                                {@link LocatedFileStatus}
     *                                entry was passed to the constructor.
     */
    @Override
    public LocatedFileStatus next() {
      if (hasNext()) {
        LocatedFileStatus s = this.status;
        status = null;
        return s;
      } else {
        throw new NoSuchElementException();
      }
    }
  }

  /**
   * Accept all entries except the base path and those which map to OBS pseudo
   * directory markers.
   */
  static class AcceptFilesOnly implements FileStatusAcceptor {
    /**
     * path to qualify.
     */
    private final Path qualifiedPath;

    AcceptFilesOnly(final Path path) {
      this.qualifiedPath = path;
    }

    /**
     * Reject a summary entry if the key path is the qualified Path, or it ends
     * with {@code "_$folder$"}.
     *
     * @param keyPath key path of the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry should be
     * generated.
     */
    @Override
    public boolean accept(final Path keyPath, final ObsObject summary) {
      return !keyPath.equals(qualifiedPath)
          && !summary.getObjectKey()
          .endsWith(OBSConstants.OBS_FOLDER_SUFFIX)
          && !OBSCommonUtils.objectRepresentsDirectory(
          summary.getObjectKey(),
          summary.getMetadata().getContentLength());
    }

    /**
     * Accept no directory paths.
     *
     * @param keyPath qualified path to the entry
     * @param prefix  common prefix in listing.
     * @return false, always.
     */
    @Override
    public boolean accept(final Path keyPath, final String prefix) {
      return false;
    }
  }

  /**
   * Accept all entries except the base path and those which map to OBS pseudo
   * directory markers.
   */
  static class AcceptAllButSelfAndS3nDirs implements FileStatusAcceptor {

    /**
     * Base path.
     */
    private final Path qualifiedPath;

    /**
     * Constructor.
     *
     * @param path an already-qualified path.
     */
    AcceptAllButSelfAndS3nDirs(final Path path) {
      this.qualifiedPath = path;
    }

    /**
     * Reject a summary entry if the key path is the qualified Path, or it ends
     * with {@code "_$folder$"}.
     *
     * @param keyPath key path of the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry should be
     * generated.)
     */
    @Override
    public boolean accept(final Path keyPath, final ObsObject summary) {
      return !keyPath.equals(qualifiedPath) && !summary.getObjectKey()
          .endsWith(OBSConstants.OBS_FOLDER_SUFFIX);
    }

    /**
     * Accept all prefixes except the one for the base path, "self".
     *
     * @param keyPath qualified path to the entry
     * @param prefix  common prefix in listing.
     * @return true if the entry is accepted (i.e. that a status entry should be
     * generated.
     */
    @Override
    public boolean accept(final Path keyPath, final String prefix) {
      return !keyPath.equals(qualifiedPath);
    }
  }

  /**
   * Wraps up object listing into a remote iterator which will ask for more
   * listing data if needed.
   *
   * <p>This is a complex operation, especially the process to determine if
   * there are more entries remaining. If there are no more results remaining in
   * the (filtered) results of the current listing request, then another request
   * is made
   * <i>and those results filtered</i> before the iterator can declare that
   * there is more data available.
   *
   * <p>The need to filter the results precludes the iterator from simply
   * declaring that if the {@link ObjectListingIterator#hasNext()} is true then
   * there are more results. Instead the next batch of results must be retrieved
   * and filtered.
   *
   * <p>What does this mean? It means that remote requests to retrieve new
   * batches of object listings are made in the {@link #hasNext()} call; the
   * {@link #next()} call simply returns the filtered results of the last
   * listing processed. However, do note that {@link #next()} calls {@link
   * #hasNext()} during its operation. This is critical to ensure that a listing
   * obtained through a sequence of {@link #next()} will complete with the same
   * set of results as a classic {@code while(it.hasNext()} loop.
   *
   * <p>Thread safety: None.
   */
  class FileStatusListingIterator implements RemoteIterator<FileStatus> {

    /**
     * Source of objects.
     */
    private final ObjectListingIterator source;

    /**
     * Filter of paths from API call.
     */
    private final PathFilter filter;

    /**
     * Filter of entries from file status.
     */
    private final FileStatusAcceptor acceptor;

    /**
     * Request batch size.
     */
    private int batchSize;

    /**
     * Iterator over the current set of results.
     */
    private ListIterator<FileStatus> statusBatchIterator;

    /**
     * Create an iterator over file status entries.
     *
     * @param listPath           the listing iterator from a listObjects call.
     * @param pathFilter         the filter on which paths to accept
     * @param fileStatusAcceptor the class/predicate to decide which entries to
     *                           accept in the listing based on the full file
     *                           status.
     * @throws IOException IO Problems
     */
    FileStatusListingIterator(
        final ObjectListingIterator listPath, final PathFilter pathFilter,
        final FileStatusAcceptor fileStatusAcceptor)
        throws IOException {
      this.source = listPath;
      this.filter = pathFilter;
      this.acceptor = fileStatusAcceptor;
      // build the first set of results. This will not trigger any
      // remote IO, assuming the source iterator is in its initial
      // iteration
      requestNextBatch();
    }

    /**
     * Report whether or not there is new data available. If there is data in
     * the local filtered list, return true. Else: request more data util that
     * condition is met, or there is no more remote listing data.
     *
     * @return true if a call to {@link #next()} will succeed.
     * @throws IOException on any failure to request next batch
     */
    @Override
    public boolean hasNext() throws IOException {
      return statusBatchIterator.hasNext() || requestNextBatch();
    }

    @Override
    public FileStatus next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return statusBatchIterator.next();
    }

    /**
     * Try to retrieve another batch. Note that for the initial batch, {@link
     * ObjectListingIterator} does not generate a request; it simply returns the
     * initial set.
     *
     * @return true if a new batch was created.
     * @throws IOException IO problems
     */
    private boolean requestNextBatch() throws IOException {
      // look for more object listing batches being available
      while (source.hasNext()) {
        // if available, retrieve it and build the next status
        if (buildNextStatusBatch(source.next())) {
          // this batch successfully generated entries matching
          // the filters/acceptors;
          // declare that the request was successful
          return true;
        } else {
          LOG.debug(
              "All entries in batch were filtered...continuing");
        }
      }
      // if this code is reached, it means that all remaining
      // object lists have been retrieved, and there are no new entries
      // to return.
      return false;
    }

    /**
     * Build the next status batch from a listing.
     *
     * @param objects the next object listing
     * @return true if this added any entries after filtering
     */
    private boolean buildNextStatusBatch(final ObjectListing objects) {
      // counters for debug logs
      int added = 0;
      int ignored = 0;
      // list to fill in with results. Initial size will be list maximum.
      List<FileStatus> stats =
          new ArrayList<>(
              objects.getObjects().size() + objects.getCommonPrefixes()
                  .size());
      // objects
      for (ObsObject summary : objects.getObjects()) {
        String key = summary.getObjectKey();
        Path keyPath = OBSCommonUtils.keyToQualifiedPath(owner, key);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: {}", keyPath,
              OBSCommonUtils.stringify(summary));
        }
        // Skip over keys that are ourselves and old OBS _$folder$ files
        if (acceptor.accept(keyPath, summary) && filter.accept(
            keyPath)) {
          FileStatus status =
              OBSCommonUtils.createFileStatus(
                  keyPath, summary,
                  owner.getDefaultBlockSize(keyPath),
                  owner.getUsername());
          LOG.debug("Adding: {}", status);
          stats.add(status);
          added++;
        } else {
          LOG.debug("Ignoring: {}", keyPath);
          ignored++;
        }
      }

      // prefixes: always directories
      for (ObsObject prefix : objects.getExtenedCommonPrefixes()) {
        String key = prefix.getObjectKey();
        Path keyPath = OBSCommonUtils.keyToQualifiedPath(owner, key);
        if (acceptor.accept(keyPath, key) && filter.accept(keyPath)) {
          long lastModified =
              prefix.getMetadata().getLastModified() == null
                  ? System.currentTimeMillis()
                  : OBSCommonUtils.dateToLong(
                      prefix.getMetadata().getLastModified());
          FileStatus status = new OBSFileStatus(keyPath, lastModified,
              lastModified, owner.getUsername());
          LOG.debug("Adding directory: {}", status);
          added++;
          stats.add(status);
        } else {
          LOG.debug("Ignoring directory: {}", keyPath);
          ignored++;
        }
      }

      // finish up
      batchSize = stats.size();
      statusBatchIterator = stats.listIterator();
      boolean hasNext = statusBatchIterator.hasNext();
      LOG.debug(
          "Added {} entries; ignored {}; hasNext={}; hasMoreObjects={}",
          added,
          ignored,
          hasNext,
          objects.isTruncated());
      return hasNext;
    }

    /**
     * Get the number of entries in the current batch.
     *
     * @return a number, possibly zero.
     */
    public int getBatchSize() {
      return batchSize;
    }
  }

  /**
   * Wraps up OBS `ListObjects` requests in a remote iterator which will ask for
   * more listing data if needed.
   *
   * <p>That is:
   *
   * <p>1. The first invocation of the {@link #next()} call will return the
   * results of the first request, the one created during the construction of
   * the instance.
   *
   * <p>2. Second and later invocations will continue the ongoing listing,
   * calling {@link OBSCommonUtils#continueListObjects} to request the next
   * batch of results.
   *
   * <p>3. The {@link #hasNext()} predicate returns true for the initial call,
   * where {@link #next()} will return the initial results. It declares that it
   * has future results iff the last executed request was truncated.
   *
   * <p>Thread safety: none.
   */
  class ObjectListingIterator implements RemoteIterator<ObjectListing> {

    /**
     * The path listed.
     */
    private final Path listPath;

    /**
     * The most recent listing results.
     */
    private ObjectListing objects;

    /**
     * Indicator that this is the first listing.
     */
    private boolean firstListing = true;

    /**
     * Count of how many listings have been requested (including initial
     * result).
     */
    private int listingCount = 1;

    /**
     * Maximum keys in a request.
     */
    private int maxKeys;

    /**
     * Constructor -calls {@link OBSCommonUtils#listObjects} on the request to
     * populate the initial set of results/fail if there was a problem talking
     * to the bucket.
     *
     * @param path    path of the listing
     * @param request initial request to make
     * @throws IOException on any failure to list objects
     */
    ObjectListingIterator(final Path path,
        final ListObjectsRequest request)
        throws IOException {
      this.listPath = path;
      this.maxKeys = owner.getMaxKeys();
      this.objects = OBSCommonUtils.listObjects(owner, request);
    }

    /**
     * Declare that the iterator has data if it is either is the initial
     * iteration or it is a later one and the last listing obtained was
     * incomplete.
     */
    @Override
    public boolean hasNext() {
      return firstListing || objects.isTruncated();
    }

    /**
     * Ask for the next listing. For the first invocation, this returns the
     * initial set, with no remote IO. For later requests, OBS will be queried,
     * hence the calls may block or fail.
     *
     * @return the next object listing.
     * @throws IOException            if a query made of OBS fails.
     * @throws NoSuchElementException if there is no more data to list.
     */
    @Override
    public ObjectListing next() throws IOException {
      if (firstListing) {
        // on the first listing, don't request more data.
        // Instead just clear the firstListing flag so that it future
        // calls will request new data.
        firstListing = false;
      } else {
        try {
          if (!objects.isTruncated()) {
            // nothing more to request: fail.
            throw new NoSuchElementException(
                "No more results in listing of " + listPath);
          }
          // need to request a new set of objects.
          LOG.debug("[{}], Requesting next {} objects under {}",
              listingCount, maxKeys, listPath);
          objects = OBSCommonUtils.continueListObjects(owner,
              objects);
          listingCount++;
          LOG.debug("New listing status: {}", this);
        } catch (ObsException e) {
          throw OBSCommonUtils.translateException("listObjects()",
              listPath, e);
        }
      }
      return objects;
    }

    @Override
    public String toString() {
      return "Object listing iterator against "
          + listPath
          + "; listing count "
          + listingCount
          + "; isTruncated="
          + objects.isTruncated();
    }

  }

  /**
   * Take a remote iterator over a set of {@link FileStatus} instances and
   * return a remote iterator of {@link LocatedFileStatus} instances.
   */
  class LocatedFileStatusIterator
      implements RemoteIterator<LocatedFileStatus> {
    /**
     * File status.
     */
    private final RemoteIterator<FileStatus> statusIterator;

    /**
     * Constructor.
     *
     * @param statusRemoteIterator an iterator over the remote status entries
     */
    LocatedFileStatusIterator(
        final RemoteIterator<FileStatus> statusRemoteIterator) {
      this.statusIterator = statusRemoteIterator;
    }

    @Override
    public boolean hasNext() throws IOException {
      return statusIterator.hasNext();
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      return OBSCommonUtils.toLocatedFileStatus(owner,
          statusIterator.next());
    }
  }
}
