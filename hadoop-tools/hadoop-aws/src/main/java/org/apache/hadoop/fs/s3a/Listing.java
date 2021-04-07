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

import javax.annotation.Nullable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.impl.AbstractStoreOperation;
import org.apache.hadoop.fs.s3a.impl.ListingOperationCallbacks;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStoreListFilesIterator;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.util.functional.RemoteIterators;

import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.StringJoiner;

import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;
import static org.apache.hadoop.fs.s3a.Constants.S3N_FOLDER_SUFFIX;
import static org.apache.hadoop.fs.s3a.S3AUtils.ACCEPT_ALL;
import static org.apache.hadoop.fs.s3a.S3AUtils.createFileStatus;
import static org.apache.hadoop.fs.s3a.S3AUtils.maybeAddTrailingSlash;
import static org.apache.hadoop.fs.s3a.S3AUtils.objectRepresentsDirectory;
import static org.apache.hadoop.fs.s3a.S3AUtils.stringify;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.pathToKey;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.util.functional.RemoteIterators.filteringRemoteIterator;
import static org.apache.hadoop.util.functional.RemoteIterators.remoteIteratorFromArray;
import static org.apache.hadoop.util.functional.RemoteIterators.remoteIteratorFromSingleton;

/**
 * Place for the S3A listing classes; keeps all the small classes under control.
 */
@InterfaceAudience.Private
public class Listing extends AbstractStoreOperation {

  private static final Logger LOG = S3AFileSystem.LOG;

  static final FileStatusAcceptor ACCEPT_ALL_BUT_S3N =
      new AcceptAllButS3nDirs();

  private final ListingOperationCallbacks listingOperationCallbacks;

  public Listing(ListingOperationCallbacks listingOperationCallbacks,
                 StoreContext storeContext) {
    super(storeContext);
    this.listingOperationCallbacks = listingOperationCallbacks;
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
  RemoteIterator<S3AFileStatus> createProvidedFileStatusIterator(
      S3AFileStatus[] fileStatuses,
      PathFilter filter,
      FileStatusAcceptor acceptor) {
    return filteringRemoteIterator(
        remoteIteratorFromArray(fileStatuses),
        status ->
            filter.accept(status.getPath()) && acceptor.accept(status));
  }

  /**
   * Create a FileStatus iterator against a provided list of file status.
   * @param fileStatuses array of file status.
   * @return the file status iterator.
   */
  @VisibleForTesting
  public static RemoteIterator<S3AFileStatus> toProvidedFileStatusIterator(
          S3AFileStatus[] fileStatuses) {
    return filteringRemoteIterator(
        remoteIteratorFromArray(fileStatuses),
        Listing.ACCEPT_ALL_BUT_S3N::accept);
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
  public FileStatusListingIterator createFileStatusListingIterator(
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
  @Retries.RetryRaw
  public FileStatusListingIterator createFileStatusListingIterator(
      Path listPath,
      S3ListRequest request,
      PathFilter filter,
      Listing.FileStatusAcceptor acceptor,
      RemoteIterator<S3AFileStatus> providedStatus) throws IOException {
    return new FileStatusListingIterator(
        createObjectListingIterator(listPath, request),
        filter,
        acceptor,
        providedStatus);
  }

  /**
   * Create an object listing iterator against a path, with a given
   * list object request.
   * @param listPath path of the listing
   * @param request initial request to make
   * @return the iterator
   * @throws IOException IO Problems
   */
  @Retries.RetryRaw
  public ObjectListingIterator createObjectListingIterator(
      final Path listPath,
      final S3ListRequest request) throws IOException {
    return new ObjectListingIterator(listPath, request);
  }

  /**
   * Create a located status iterator over a file status iterator.
   * @param statusIterator an iterator over the remote status entries
   * @return a new remote iterator
   */
  @VisibleForTesting
  public RemoteIterator<S3ALocatedFileStatus> createLocatedFileStatusIterator(
      RemoteIterator<S3AFileStatus> statusIterator) {
    return RemoteIterators.mappingRemoteIterator(
        statusIterator,
        listingOperationCallbacks::toLocatedFileStatus);
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
  RemoteIterator<S3ALocatedFileStatus> createTombstoneReconcilingIterator(
      RemoteIterator<S3ALocatedFileStatus> iterator,
      @Nullable Set<Path> tombstones) {
    if (tombstones == null || tombstones.isEmpty()) {
      // no need to filter.
      return iterator;
    } else {
      return filteringRemoteIterator(
          iterator,
          candidate -> !tombstones.contains(candidate.getPath()));
    }
  }

  /**
   * Create a remote iterator from a single status entry.
   * @param status status
   * @return iterator.
   */
  public RemoteIterator<S3ALocatedFileStatus> createSingleStatusIterator(
      S3ALocatedFileStatus status) {
    return remoteIteratorFromSingleton(status);
  }

  /**
   * List files under a path assuming the path to be a directory.
   * @param path input path.
   * @param recursive recursive listing?
   * @param acceptor file status filter
   * @param collectTombstones should tombstones be collected from S3Guard?
   * @param forceNonAuthoritativeMS forces metadata store to act like non
   *                                authoritative. This is useful when
   *                                listFiles output is used by import tool.
   * @return an iterator over listing.
   * @throws IOException any exception.
   */
  public RemoteIterator<S3ALocatedFileStatus> getListFilesAssumingDir(
          Path path,
          boolean recursive, Listing.FileStatusAcceptor acceptor,
          boolean collectTombstones,
          boolean forceNonAuthoritativeMS) throws IOException {

    String key = maybeAddTrailingSlash(pathToKey(path));
    String delimiter = recursive ? null : "/";
    if (recursive) {
      LOG.debug("Recursive list of all entries under {}", key);
    } else {
      LOG.debug("Requesting all entries under {} with delimiter '{}'",
          key, delimiter);
    }
    final RemoteIterator<S3AFileStatus> cachedFilesIterator;
    final Set<Path> tombstones;
    boolean allowAuthoritative = listingOperationCallbacks
            .allowAuthoritative(path);
    if (recursive) {
      final PathMetadata pm = getStoreContext()
              .getMetadataStore()
              .get(path, true);
      if (pm != null) {
        if (pm.isDeleted()) {
          OffsetDateTime deletedAt = OffsetDateTime
                  .ofInstant(Instant.ofEpochMilli(
                          pm.getFileStatus().getModificationTime()),
                          ZoneOffset.UTC);
          throw new FileNotFoundException("Path " + path + " is recorded as " +
                  "deleted by S3Guard at " + deletedAt);
        }
      }
      MetadataStoreListFilesIterator metadataStoreListFilesIterator =
              new MetadataStoreListFilesIterator(
                      getStoreContext().getMetadataStore(),
                      pm,
                      allowAuthoritative);
      tombstones = metadataStoreListFilesIterator.listTombstones();
      // if all of the below is true
      //  - authoritative access is allowed for this metadatastore
      //  for this directory,
      //  - all the directory listings are authoritative on the client
      //  - the caller does not force non-authoritative access
      // return the listing without any further s3 access
      if (!forceNonAuthoritativeMS &&
              allowAuthoritative &&
              metadataStoreListFilesIterator.isRecursivelyAuthoritative()) {
        S3AFileStatus[] statuses = S3AUtils.iteratorToStatuses(
                metadataStoreListFilesIterator, tombstones);
        cachedFilesIterator = createProvidedFileStatusIterator(
                statuses, ACCEPT_ALL, acceptor);
        return createLocatedFileStatusIterator(cachedFilesIterator);
      }
      cachedFilesIterator = metadataStoreListFilesIterator;
    } else {
      DirListingMetadata meta =
              S3Guard.listChildrenWithTtl(
                      getStoreContext().getMetadataStore(),
                      path,
                      listingOperationCallbacks.getUpdatedTtlTimeProvider(),
                      allowAuthoritative);
      if (meta != null) {
        tombstones = meta.listTombstones();
      } else {
        tombstones = null;
      }
      cachedFilesIterator = createProvidedFileStatusIterator(
              S3Guard.dirMetaToStatuses(meta), ACCEPT_ALL, acceptor);
      if (allowAuthoritative && meta != null && meta.isAuthoritative()) {
        // metadata listing is authoritative, so return it directly
        return createLocatedFileStatusIterator(cachedFilesIterator);
      }
    }
    return createTombstoneReconcilingIterator(
            createLocatedFileStatusIterator(
                    createFileStatusListingIterator(path,
                                    listingOperationCallbacks
                                    .createListObjectsRequest(key, delimiter),
                            ACCEPT_ALL,
                            acceptor,
                            cachedFilesIterator)),
            collectTombstones ? tombstones : null);
  }

  /**
   * Generate list located status for a directory.
   * Also performing tombstone reconciliation for guarded directories.
   * @param dir directory to check.
   * @param filter a path filter.
   * @return an iterator that traverses statuses of the given dir.
   * @throws IOException in case of failure.
   */
  public RemoteIterator<S3ALocatedFileStatus> getLocatedFileStatusIteratorForDir(
          Path dir, PathFilter filter) throws IOException {
    final String key = maybeAddTrailingSlash(pathToKey(dir));
    final Listing.FileStatusAcceptor acceptor =
            new Listing.AcceptAllButSelfAndS3nDirs(dir);
    boolean allowAuthoritative = listingOperationCallbacks
            .allowAuthoritative(dir);
    DirListingMetadata meta =
            S3Guard.listChildrenWithTtl(getStoreContext().getMetadataStore(),
                    dir,
                    listingOperationCallbacks
                            .getUpdatedTtlTimeProvider(),
                    allowAuthoritative);
    Set<Path> tombstones = meta != null
            ? meta.listTombstones()
            : null;
    final RemoteIterator<S3AFileStatus> cachedFileStatusIterator =
            createProvidedFileStatusIterator(
                    S3Guard.dirMetaToStatuses(meta), filter, acceptor);
    return (allowAuthoritative && meta != null
            && meta.isAuthoritative())
            ? createLocatedFileStatusIterator(
            cachedFileStatusIterator)
            : createTombstoneReconcilingIterator(
            createLocatedFileStatusIterator(
                    createFileStatusListingIterator(dir,
                            listingOperationCallbacks
                                    .createListObjectsRequest(key, "/"),
                            filter,
                            acceptor,
                            cachedFileStatusIterator)),
            tombstones);
  }

  /**
   * Calculate list of file statuses assuming path
   * to be a non-empty directory.
   * @param path input path.
   * @return Triple of file statuses, metaData, auth flag.
   * @throws IOException Any IO problems.
   */
  public Triple<RemoteIterator<S3AFileStatus>, DirListingMetadata, Boolean>
        getFileStatusesAssumingNonEmptyDir(Path path)
          throws IOException {
    String key = pathToKey(path);
    List<S3AFileStatus> result;
    if (!key.isEmpty()) {
      key = key + '/';
    }

    boolean allowAuthoritative = listingOperationCallbacks
            .allowAuthoritative(path);
    DirListingMetadata dirMeta =
            S3Guard.listChildrenWithTtl(
                    getStoreContext().getMetadataStore(),
                    path,
                    listingOperationCallbacks.getUpdatedTtlTimeProvider(),
                    allowAuthoritative);
    // In auth mode return directly with auth flag.
    if (allowAuthoritative && dirMeta != null && dirMeta.isAuthoritative()) {
      RemoteIterator<S3AFileStatus> mfsItr = createProvidedFileStatusIterator(
              S3Guard.dirMetaToStatuses(dirMeta),
              ACCEPT_ALL,
              Listing.ACCEPT_ALL_BUT_S3N);
      return Triple.of(mfsItr,
              dirMeta, Boolean.TRUE);
    }

    S3ListRequest request = createListObjectsRequest(key, "/");
    LOG.debug("listStatus: doing listObjects for directory {}", key);

    FileStatusListingIterator filesItr = createFileStatusListingIterator(
            path,
            request,
            ACCEPT_ALL,
            new Listing.AcceptAllButSelfAndS3nDirs(path));

    // return the results obtained from s3.
    return Triple.of(
            filesItr,
            dirMeta,
            Boolean.FALSE);
  }

  public S3ListRequest createListObjectsRequest(String key, String delimiter) {
    return listingOperationCallbacks.createListObjectsRequest(key, delimiter);
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
      implements RemoteIterator<S3AFileStatus>, IOStatisticsSource {

    /** Source of objects. */
    private final ObjectListingIterator source;
    /** Filter of paths from API call. */
    private final PathFilter filter;
    /** Filter of entries from file status. */
    private final FileStatusAcceptor acceptor;
    /** request batch size. */
    private int batchSize;
    /** Iterator over the current set of results. */
    private ListIterator<S3AFileStatus> statusBatchIterator;

    private final Map<Path, S3AFileStatus> providedStatus;
    private Iterator<S3AFileStatus> providedStatusIterator;

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
    @Retries.RetryTranslated
    FileStatusListingIterator(ObjectListingIterator source,
        PathFilter filter,
        FileStatusAcceptor acceptor,
        @Nullable RemoteIterator<S3AFileStatus> providedStatus)
        throws IOException {
      this.source = source;
      this.filter = filter;
      this.acceptor = acceptor;
      this.providedStatus = new HashMap<>();
      for (; providedStatus != null && providedStatus.hasNext();) {
        final S3AFileStatus status = providedStatus.next();
        Path path = status.getPath();
        if (filter.accept(path) && acceptor.accept(status)) {
          this.providedStatus.put(path, status);
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
    @Retries.RetryTranslated
    public boolean hasNext() throws IOException {
      return sourceHasNext() || providedStatusIterator.hasNext();
    }

    @Retries.RetryTranslated
    private boolean sourceHasNext() throws IOException {
      if (statusBatchIterator.hasNext() || requestNextBatch()) {
        return true;
      } else {
        // turn to file status that are only in provided list
        if (providedStatusIterator == null) {
          LOG.debug("Start iterating the provided status.");
          providedStatusIterator = providedStatus.values().iterator();
        }
        return false;
      }
    }

    @Override
    @Retries.RetryTranslated
    public S3AFileStatus next() throws IOException {
      final S3AFileStatus status;
      if (sourceHasNext()) {
        status = statusBatchIterator.next();
        // We remove from provided map the file status listed by S3 so that
        // this does not return duplicate items.

        // The provided status is returned as it is assumed to have the better
        // metadata (i.e. the eTag and versionId from S3Guard)
        S3AFileStatus provided = providedStatus.remove(status.getPath());
        if (provided != null) {
          LOG.debug(
              "Removed and returned the status from provided file status {}",
              status);
          return provided;
        }
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
    @Retries.RetryTranslated
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
      List<S3AFileStatus> stats = new ArrayList<>(
          objects.getObjectSummaries().size() +
              objects.getCommonPrefixes().size());
      // objects
      for (S3ObjectSummary summary : objects.getObjectSummaries()) {
        String key = summary.getKey();
        Path keyPath = getStoreContext().getContextAccessors().keyToPath(key);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: {}", keyPath, stringify(summary));
        }
        // Skip over keys that are ourselves and old S3N _$folder$ files
        if (acceptor.accept(keyPath, summary) && filter.accept(keyPath)) {
          S3AFileStatus status = createFileStatus(keyPath, summary,
                  listingOperationCallbacks.getDefaultBlockSize(keyPath),
                  getStoreContext().getUsername(),
              summary.getETag(), null);
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
        Path keyPath = getStoreContext()
                .getContextAccessors()
                .keyToPath(prefix);
        if (acceptor.accept(keyPath, prefix) && filter.accept(keyPath)) {
          S3AFileStatus status = new S3AFileStatus(Tristate.FALSE, keyPath,
              getStoreContext().getUsername());
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

    /**
     * Return any IOStatistics provided by the underlying stream.
     * @return IO stats from the inner stream.
     */
    @Override
    public IOStatistics getIOStatistics() {
      return source.getIOStatistics();
    }

    @Override
    public String toString() {
      return new StringJoiner(", ",
          FileStatusListingIterator.class.getSimpleName() + "[", "]")
          .add(source.toString())
          .toString();
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
  class ObjectListingIterator implements RemoteIterator<S3ListResult>,
      IOStatisticsSource {

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

    private final IOStatisticsStore iostats;

    /**
     * Future to store current batch listing result.
     */
    private CompletableFuture<S3ListResult> s3ListResultFuture;

    /**
     * Result of previous batch.
     */
    private S3ListResult objectsPrev;

    /**
     * Constructor -calls `listObjects()` on the request to populate the
     * initial set of results/fail if there was a problem talking to the bucket.
     * @param listPath path of the listing
     * @param request initial request to make
     * @throws IOException if listObjects raises one.
     */
    @Retries.RetryRaw
    ObjectListingIterator(
        Path listPath,
        S3ListRequest request) throws IOException {
      this.listPath = listPath;
      this.maxKeys = listingOperationCallbacks.getMaxKeys();
      this.request = request;
      this.objectsPrev = null;
      this.iostats = iostatisticsStore()
          .withDurationTracking(OBJECT_LIST_REQUEST)
          .withDurationTracking(OBJECT_CONTINUE_LIST_REQUEST)
          .build();
      this.s3ListResultFuture = listingOperationCallbacks
          .listObjectsAsync(request, iostats);
    }

    /**
     * Declare that the iterator has data if it is either is the initial
     * iteration or it is a later one and the last listing obtained was
     * incomplete.
     * @throws IOException never: there is no IO in this operation.
     */
    @Override
    public boolean hasNext() throws IOException {
      return firstListing ||
              (objectsPrev != null && objectsPrev.isTruncated());
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
    @Retries.RetryTranslated
    public S3ListResult next() throws IOException {
      if (firstListing) {
        // clear the firstListing flag for future calls.
        firstListing = false;
        // Calculating the result of last async list call.
        objects = awaitFuture(s3ListResultFuture);
        fetchNextBatchAsyncIfPresent();
      } else {
        try {
          if (objectsPrev!= null && !objectsPrev.isTruncated()) {
            // nothing more to request: fail.
            throw new NoSuchElementException("No more results in listing of "
                    + listPath);
          }
          // Calculating the result of last async list call.
          objects = awaitFuture(s3ListResultFuture);
          // Requesting next batch of results.
          fetchNextBatchAsyncIfPresent();
          listingCount++;
          LOG.debug("New listing status: {}", this);
        } catch (AmazonClientException e) {
          throw translateException("listObjects()", listPath, e);
        }
      }
      // Storing the current result to be used by hasNext() call.
      objectsPrev = objects;
      return objectsPrev;
    }

    /**
     * If there are more listings present, call for next batch async.
     * @throws IOException
     */
    private void fetchNextBatchAsyncIfPresent() throws IOException {
      if (objects.isTruncated()) {
        LOG.debug("[{}], Requesting next {} objects under {}",
                listingCount, maxKeys, listPath);
        s3ListResultFuture = listingOperationCallbacks
                .continueListObjectsAsync(request, objects, iostats);
      }
    }

    @Override
    public String toString() {
      return "Object listing iterator against " + listPath
          + "; listing count "+ listingCount
          + "; isTruncated=" + objects.isTruncated()
          + "; " + iostats;
    }

    @Override
    public IOStatistics getIOStatistics() {
      return iostats;
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
  public static class AcceptAllButSelfAndS3nDirs implements FileStatusAcceptor {

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

  public static RemoteIterator<LocatedFileStatus> toLocatedFileStatusIterator(
      RemoteIterator<? extends LocatedFileStatus> iterator) {
    return (RemoteIterator < LocatedFileStatus >) iterator;
  }

}
