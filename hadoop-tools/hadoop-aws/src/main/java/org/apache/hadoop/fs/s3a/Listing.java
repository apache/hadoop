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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.hadoop.fs.s3a.Constants.S3N_FOLDER_SUFFIX;
import static org.apache.hadoop.fs.s3a.S3AUtils.createFileStatus;
import static org.apache.hadoop.fs.s3a.S3AUtils.objectRepresentsDirectory;
import static org.apache.hadoop.fs.s3a.S3AUtils.stringify;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;

/**
 * Place for the S3A listing classes; keeps all the small classes under control.
 */
public class Listing {

  private final S3AFileSystem owner;
  private static final Logger LOG = S3AFileSystem.LOG;

  public Listing(S3AFileSystem owner) {
    this.owner = owner;
  }

  /**
   * Create a FileStatus iterator against a provided list of file status, with
   * a given status filter.
   *
   * @param fileStatuses the provided list of file status. NO remote calls.
   * @param filter file path filter on which paths to accept
   * @param acceptor the file status acceptor
   * @return the file status iterator
   */
  ProvidedFileStatusIterator createProvidedFileStatusIterator(
      FileStatus[] fileStatuses,
      PathFilter filter,
      FileStatusAcceptor acceptor) {
    return new ProvidedFileStatusIterator(fileStatuses, filter, acceptor);
  }

  /**
   * Create a FileStatus iterator against a path, with a given list object
   * request.
   *
   * @param listPath path of the listing
   * @param request initial request to make
   * @param filter the filter on which paths to accept
   * @param acceptor the class/predicate to decide which entries to accept
   * in the listing based on the full file status.
   * @return the iterator
   * @throws IOException IO Problems
   */
  FileStatusListingIterator createFileStatusListingIterator(
      Path listPath,
      S3ListRequest request,
      PathFilter filter,
      Listing.FileStatusAcceptor acceptor) throws IOException {
    return createFileStatusListingIterator(listPath, request, filter, acceptor,
        null);
  }

  /**
   * Create a FileStatus iterator against a path, with a given
   * list object request.
   * @param listPath path of the listing
   * @param request initial request to make
   * @param filter the filter on which paths to accept
   * @param acceptor the class/predicate to decide which entries to accept
   * in the listing based on the full file status.
   * @param providedStatus the provided list of file status, which may contain
   *                       items that are not listed from source.
   * @return the iterator
   * @throws IOException IO Problems
   */
  FileStatusListingIterator createFileStatusListingIterator(
      Path listPath,
      S3ListRequest request,
      PathFilter filter,
      Listing.FileStatusAcceptor acceptor,
      RemoteIterator<FileStatus> providedStatus) throws IOException {
    return new FileStatusListingIterator(
        new ObjectListingIterator(listPath, request),
        filter,
        acceptor,
        providedStatus);
  }

  /**
   * Create a located status iterator over a file status iterator.
   * @param statusIterator an iterator over the remote status entries
   * @return a new remote iterator
   */
  @VisibleForTesting
  LocatedFileStatusIterator createLocatedFileStatusIterator(
      RemoteIterator<FileStatus> statusIterator) {
    return new LocatedFileStatusIterator(statusIterator);
  }

  /**
   * Create an located status iterator that wraps another to filter out a set
   * of recently deleted items.
   * @param iterator an iterator over the remote located status entries.
   * @param tombstones set of paths that are recently deleted and should be
   *                   filtered.
   * @return a new remote iterator.
   */
  @VisibleForTesting
  TombstoneReconcilingIterator createTombstoneReconcilingIterator(
      RemoteIterator<LocatedFileStatus> iterator, Set<Path> tombstones) {
    return new TombstoneReconcilingIterator(iterator, tombstones);
  }

  /**
   * Interface to implement by the logic deciding whether to accept a summary
   * entry or path as a valid file or directory.
   */
  interface FileStatusAcceptor {

    /**
     * Predicate to decide whether or not to accept a summary entry.
     * @param keyPath qualified path to the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.
     */
    boolean accept(Path keyPath, S3ObjectSummary summary);

    /**
     * Predicate to decide whether or not to accept a prefix.
     * @param keyPath qualified path to the entry
     * @param commonPrefix the prefix
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.)
     */
    boolean accept(Path keyPath, String commonPrefix);

    /**
     * Predicate to decide whether or not to accept a file status.
     * @param status file status containing file path information
     * @return true if the status is accepted else false
     */
    boolean accept(FileStatus status);
  }

  /**
   * A remote iterator which only iterates over a single `LocatedFileStatus`
   * value.
   *
   * If the status value is null, the iterator declares that it has no data.
   * This iterator is used to handle {@link S3AFileSystem#listStatus} calls
   * where the path handed in refers to a file, not a directory: this is the
   * iterator returned.
   */
  static final class SingleStatusRemoteIterator
      implements RemoteIterator<LocatedFileStatus> {

    /**
     * The status to return; set to null after the first iteration.
     */
    private LocatedFileStatus status;

    /**
     * Constructor.
     * @param status status value: may be null, in which case
     * the iterator is empty.
     */
    public SingleStatusRemoteIterator(LocatedFileStatus status) {
      this.status = status;
    }

    /**
     * {@inheritDoc}
     * @return true if there is a file status to return: this is always false
     * for the second iteration, and may be false for the first.
     * @throws IOException never
     */
    @Override
    public boolean hasNext() throws IOException {
      return status != null;
    }

    /**
     * {@inheritDoc}
     * @return the non-null status element passed in when the instance was
     * constructed, if it ha not already been retrieved.
     * @throws IOException never
     * @throws NoSuchElementException if this is the second call, or it is
     * the first call and a null {@link LocatedFileStatus} entry was passed
     * to the constructor.
     */
    @Override
    public LocatedFileStatus next() throws IOException {
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
   * This wraps up a provided non-null list of file status as a remote iterator.
   *
   * It firstly filters the provided list and later {@link #next} call will get
   * from the filtered list. This suffers from scalability issues if the
   * provided list is too large.
   *
   * There is no remote data to fetch.
   */
  static class ProvidedFileStatusIterator
      implements RemoteIterator<FileStatus> {
    private final ArrayList<FileStatus> filteredStatusList;
    private int index = 0;

    ProvidedFileStatusIterator(FileStatus[] fileStatuses, PathFilter filter,
        FileStatusAcceptor acceptor) {
      Preconditions.checkArgument(fileStatuses != null, "Null status list!");

      filteredStatusList = new ArrayList<>(fileStatuses.length);
      for (FileStatus status : fileStatuses) {
        if (filter.accept(status.getPath()) && acceptor.accept(status)) {
          filteredStatusList.add(status);
        }
      }
      filteredStatusList.trimToSize();
    }

    @Override
    public boolean hasNext() throws IOException {
      return index < filteredStatusList.size();
    }

    @Override
    public FileStatus next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return filteredStatusList.get(index++);
    }
  }

  /**
   * Wraps up object listing into a remote iterator which will ask for more
   * listing data if needed.
   *
   * This is a complex operation, especially the process to determine
   * if there are more entries remaining. If there are no more results
   * remaining in the (filtered) results of the current listing request, then
   * another request is made <i>and those results filtered</i> before the
   * iterator can declare that there is more data available.
   *
   * The need to filter the results precludes the iterator from simply
   * declaring that if the {@link ObjectListingIterator#hasNext()}
   * is true then there are more results. Instead the next batch of results must
   * be retrieved and filtered.
   *
   * What does this mean? It means that remote requests to retrieve new
   * batches of object listings are made in the {@link #hasNext()} call;
   * the {@link #next()} call simply returns the filtered results of the last
   * listing processed. However, do note that {@link #next()} calls
   * {@link #hasNext()} during its operation. This is critical to ensure
   * that a listing obtained through a sequence of {@link #next()} will
   * complete with the same set of results as a classic
   * {@code while(it.hasNext()} loop.
   *
   * Thread safety: None.
   */
  class FileStatusListingIterator
      implements RemoteIterator<FileStatus> {

    /** Source of objects. */
    private final ObjectListingIterator source;
    /** Filter of paths from API call. */
    private final PathFilter filter;
    /** Filter of entries from file status. */
    private final FileStatusAcceptor acceptor;
    /** request batch size. */
    private int batchSize;
    /** Iterator over the current set of results. */
    private ListIterator<FileStatus> statusBatchIterator;

    private final Set<FileStatus> providedStatus;
    private Iterator<FileStatus> providedStatusIterator;

    /**
     * Create an iterator over file status entries.
     * @param source the listing iterator from a listObjects call.
     * @param filter the filter on which paths to accept
     * @param acceptor the class/predicate to decide which entries to accept
     * in the listing based on the full file status.
     * @param providedStatus the provided list of file status, which may contain
     *                       items that are not listed from source.
     * @throws IOException IO Problems
     */
    FileStatusListingIterator(ObjectListingIterator source,
        PathFilter filter,
        FileStatusAcceptor acceptor,
        RemoteIterator<FileStatus> providedStatus) throws IOException {
      this.source = source;
      this.filter = filter;
      this.acceptor = acceptor;
      this.providedStatus = new HashSet<>();
      for (; providedStatus != null && providedStatus.hasNext();) {
        final FileStatus status = providedStatus.next();
        if (filter.accept(status.getPath()) && acceptor.accept(status)) {
          this.providedStatus.add(status);
        }
      }
      // build the first set of results. This will not trigger any
      // remote IO, assuming the source iterator is in its initial
      // iteration
      requestNextBatch();
    }

    /**
     * Report whether or not there is new data available.
     * If there is data in the local filtered list, return true.
     * Else: request more data util that condition is met, or there
     * is no more remote listing data.
     * Lastly, return true if the {@code providedStatusIterator}
     * has left items.
     * @return true if a call to {@link #next()} will succeed.
     * @throws IOException
     */
    @Override
    public boolean hasNext() throws IOException {
      return sourceHasNext() || providedStatusIterator.hasNext();
    }

    private boolean sourceHasNext() throws IOException {
      if (statusBatchIterator.hasNext() || requestNextBatch()) {
        return true;
      } else {
        // turn to file status that are only in provided list
        if (providedStatusIterator == null) {
          LOG.debug("Start iterating the provided status.");
          providedStatusIterator = providedStatus.iterator();
        }
        return false;
      }
    }

    @Override
    public FileStatus next() throws IOException {
      final FileStatus status;
      if (sourceHasNext()) {
        status = statusBatchIterator.next();
        // We remove from provided list the file status listed by S3 so that
        // this does not return duplicate items.
        LOG.debug("Removing the status from provided file status {}", status);
        providedStatus.remove(status);
      } else {
        if (providedStatusIterator.hasNext()) {
          status = providedStatusIterator.next();
          LOG.debug("Returning provided file status {}", status);
        } else {
          throw new NoSuchElementException();
        }
      }
      return status;
    }

    /**
     * Try to retrieve another batch.
     * Note that for the initial batch,
     * {@link ObjectListingIterator} does not generate a request;
     * it simply returns the initial set.
     *
     * @return true if a new batch was created.
     * @throws IOException IO problems
     */
    private boolean requestNextBatch() throws IOException {
      // look for more object listing batches being available
      while (source.hasNext()) {
        // if available, retrieve it and build the next status
        if (buildNextStatusBatch(source.next())) {
          // this batch successfully generated entries matching the filters/
          // acceptors; declare that the request was successful
          return true;
        } else {
          LOG.debug("All entries in batch were filtered...continuing");
        }
      }
      // if this code is reached, it means that all remaining
      // object lists have been retrieved, and there are no new entries
      // to return.
      return false;
    }

    /**
     * Build the next status batch from a listing.
     * @param objects the next object listing
     * @return true if this added any entries after filtering
     */
    private boolean buildNextStatusBatch(S3ListResult objects) {
      // counters for debug logs
      int added = 0, ignored = 0;
      // list to fill in with results. Initial size will be list maximum.
      List<FileStatus> stats = new ArrayList<>(
          objects.getObjectSummaries().size() +
              objects.getCommonPrefixes().size());
      // objects
      for (S3ObjectSummary summary : objects.getObjectSummaries()) {
        String key = summary.getKey();
        Path keyPath = owner.keyToQualifiedPath(key);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: {}", keyPath, stringify(summary));
        }
        // Skip over keys that are ourselves and old S3N _$folder$ files
        if (acceptor.accept(keyPath, summary) && filter.accept(keyPath)) {
          FileStatus status = createFileStatus(keyPath, summary,
              owner.getDefaultBlockSize(keyPath), owner.getUsername());
          LOG.debug("Adding: {}", status);
          stats.add(status);
          added++;
        } else {
          LOG.debug("Ignoring: {}", keyPath);
          ignored++;
        }
      }

      // prefixes: always directories
      for (String prefix : objects.getCommonPrefixes()) {
        Path keyPath = owner.keyToQualifiedPath(prefix);
        if (acceptor.accept(keyPath, prefix) && filter.accept(keyPath)) {
          FileStatus status = new S3AFileStatus(Tristate.FALSE, keyPath,
              owner.getUsername());
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
      LOG.debug("Added {} entries; ignored {}; hasNext={}; hasMoreObjects={}",
          added, ignored, hasNext, objects.isTruncated());
      return hasNext;
    }

    /**
     * Get the number of entries in the current batch.
     * @return a number, possibly zero.
     */
    public int getBatchSize() {
      return batchSize;
    }
  }

  /**
   * Wraps up AWS `ListObjects` requests in a remote iterator
   * which will ask for more listing data if needed.
   *
   * That is:
   *
   * 1. The first invocation of the {@link #next()} call will return the results
   * of the first request, the one created during the construction of the
   * instance.
   *
   * 2. Second and later invocations will continue the ongoing listing,
   * calling {@link S3AFileSystem#continueListObjects} to request the next
   * batch of results.
   *
   * 3. The {@link #hasNext()} predicate returns true for the initial call,
   * where {@link #next()} will return the initial results. It declares
   * that it has future results iff the last executed request was truncated.
   *
   * Thread safety: none.
   */
  class ObjectListingIterator implements RemoteIterator<S3ListResult> {

    /** The path listed. */
    private final Path listPath;

    /** The most recent listing results. */
    private S3ListResult objects;

    /** The most recent listing request. */
    private S3ListRequest request;

    /** Indicator that this is the first listing. */
    private boolean firstListing = true;

    /**
     * Count of how many listings have been requested
     * (including initial result).
     */
    private int listingCount = 1;

    /**
     * Maximum keys in a request.
     */
    private int maxKeys;

    /**
     * Constructor -calls `listObjects()` on the request to populate the
     * initial set of results/fail if there was a problem talking to the bucket.
     * @param listPath path of the listing
     * @param request initial request to make
     * */
    ObjectListingIterator(
        Path listPath,
        S3ListRequest request) {
      this.listPath = listPath;
      this.maxKeys = owner.getMaxKeys();
      this.objects = owner.listObjects(request);
      this.request = request;
    }

    /**
     * Declare that the iterator has data if it is either is the initial
     * iteration or it is a later one and the last listing obtained was
     * incomplete.
     * @throws IOException never: there is no IO in this operation.
     */
    @Override
    public boolean hasNext() throws IOException {
      return firstListing || objects.isTruncated();
    }

    /**
     * Ask for the next listing.
     * For the first invocation, this returns the initial set, with no
     * remote IO. For later requests, S3 will be queried, hence the calls
     * may block or fail.
     * @return the next object listing.
     * @throws IOException if a query made of S3 fails.
     * @throws NoSuchElementException if there is no more data to list.
     */
    @Override
    public S3ListResult next() throws IOException {
      if (firstListing) {
        // on the first listing, don't request more data.
        // Instead just clear the firstListing flag so that it future calls
        // will request new data.
        firstListing = false;
      } else {
        try {
          if (!objects.isTruncated()) {
            // nothing more to request: fail.
            throw new NoSuchElementException("No more results in listing of "
                + listPath);
          }
          // need to request a new set of objects.
          LOG.debug("[{}], Requesting next {} objects under {}",
              listingCount, maxKeys, listPath);
          objects = owner.continueListObjects(request, objects);
          listingCount++;
          LOG.debug("New listing status: {}", this);
        } catch (AmazonClientException e) {
          throw translateException("listObjects()", listPath, e);
        }
      }
      return objects;
    }

    @Override
    public String toString() {
      return "Object listing iterator against " + listPath
          + "; listing count "+ listingCount
          + "; isTruncated=" + objects.isTruncated();
    }

    /**
     * Get the path listed.
     * @return the path used in this listing.
     */
    public Path getListPath() {
      return listPath;
    }

    /**
     * Get the count of listing requests.
     * @return the counter of requests made (including the initial lookup).
     */
    public int getListingCount() {
      return listingCount;
    }
  }

  /**
   * Accept all entries except the base path and those which map to S3N
   * pseudo directory markers.
   */
  static class AcceptFilesOnly implements FileStatusAcceptor {
    private final Path qualifiedPath;

    public AcceptFilesOnly(Path qualifiedPath) {
      this.qualifiedPath = qualifiedPath;
    }

    /**
     * Reject a summary entry if the key path is the qualified Path, or
     * it ends with {@code "_$folder$"}.
     * @param keyPath key path of the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.
     */
    @Override
    public boolean accept(Path keyPath, S3ObjectSummary summary) {
      return !keyPath.equals(qualifiedPath)
          && !summary.getKey().endsWith(S3N_FOLDER_SUFFIX)
          && !objectRepresentsDirectory(summary.getKey(), summary.getSize());
    }

    /**
     * Accept no directory paths.
     * @param keyPath qualified path to the entry
     * @param prefix common prefix in listing.
     * @return false, always.
     */
    @Override
    public boolean accept(Path keyPath, String prefix) {
      return false;
    }

    @Override
    public boolean accept(FileStatus status) {
      return (status != null) && status.isFile();
    }
  }

  /**
   * Take a remote iterator over a set of {@link FileStatus} instances and
   * return a remote iterator of {@link LocatedFileStatus} instances.
   */
  class LocatedFileStatusIterator
      implements RemoteIterator<LocatedFileStatus> {
    private final RemoteIterator<FileStatus> statusIterator;

    /**
     * Constructor.
     * @param statusIterator an iterator over the remote status entries
     */
    LocatedFileStatusIterator(RemoteIterator<FileStatus> statusIterator) {
      this.statusIterator = statusIterator;
    }

    @Override
    public boolean hasNext() throws IOException {
      return statusIterator.hasNext();
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      return owner.toLocatedFileStatus(statusIterator.next());
    }
  }

  /**
   * Wraps another iterator and filters out files that appear in the provided
   * set of tombstones.  Will read ahead in the iterator when necessary to
   * ensure that emptiness is detected early enough if only deleted objects
   * remain in the source iterator.
   */
  static class TombstoneReconcilingIterator implements
      RemoteIterator<LocatedFileStatus> {
    private LocatedFileStatus next = null;
    private final RemoteIterator<LocatedFileStatus> iterator;
    private final Set<Path> tombstones;

    /**
     * @param iterator Source iterator to filter
     * @param tombstones set of tombstone markers to filter out of results
     */
    TombstoneReconcilingIterator(RemoteIterator<LocatedFileStatus>
        iterator, Set<Path> tombstones) {
      this.iterator = iterator;
      if (tombstones != null) {
        this.tombstones = tombstones;
      } else {
        this.tombstones = Collections.EMPTY_SET;
      }
    }

    private boolean fetch() throws IOException {
      while (next == null && iterator.hasNext()) {
        LocatedFileStatus candidate = iterator.next();
        if (!tombstones.contains(candidate.getPath())) {
          next = candidate;
          return true;
        }
      }
      return false;
    }

    public boolean hasNext() throws IOException {
      if (next != null) {
        return true;
      }
      return fetch();
    }

    public LocatedFileStatus next() throws IOException {
      if (hasNext()) {
        LocatedFileStatus result = next;
        next = null;
        fetch();
        return result;
      }
      throw new NoSuchElementException();
    }
  }

  /**
   * Accept all entries except those which map to S3N pseudo directory markers.
   */
  static class AcceptAllButS3nDirs implements FileStatusAcceptor {

    public boolean accept(Path keyPath, S3ObjectSummary summary) {
      return !summary.getKey().endsWith(S3N_FOLDER_SUFFIX);
    }

    public boolean accept(Path keyPath, String prefix) {
      return !keyPath.toString().endsWith(S3N_FOLDER_SUFFIX);
    }

    public boolean accept(FileStatus status) {
      return !status.getPath().toString().endsWith(S3N_FOLDER_SUFFIX);
    }

  }

  /**
   * Accept all entries except the base path and those which map to S3N
   * pseudo directory markers.
   */
  static class AcceptAllButSelfAndS3nDirs implements FileStatusAcceptor {

    /** Base path. */
    private final Path qualifiedPath;

    /**
     * Constructor.
     * @param qualifiedPath an already-qualified path.
     */
    public AcceptAllButSelfAndS3nDirs(Path qualifiedPath) {
      this.qualifiedPath = qualifiedPath;
    }

    /**
     * Reject a summary entry if the key path is the qualified Path, or
     * it ends with {@code "_$folder$"}.
     * @param keyPath key path of the entry
     * @param summary summary entry
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.)
     */
    @Override
    public boolean accept(Path keyPath, S3ObjectSummary summary) {
      return !keyPath.equals(qualifiedPath) &&
          !summary.getKey().endsWith(S3N_FOLDER_SUFFIX);
    }

    /**
     * Accept all prefixes except the one for the base path, "self".
     * @param keyPath qualified path to the entry
     * @param prefix common prefix in listing.
     * @return true if the entry is accepted (i.e. that a status entry
     * should be generated.
     */
    @Override
    public boolean accept(Path keyPath, String prefix) {
      return !keyPath.equals(qualifiedPath);
    }

    @Override
    public boolean accept(FileStatus status) {
      return (status != null) && !status.getPath().equals(qualifiedPath);
    }
  }

  /**
   * A Path filter which accepts all filenames.
   */
  static final PathFilter ACCEPT_ALL = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return true;
    }

    @Override
    public String toString() {
      return "ACCEPT_ALL";
    }
  };

}
