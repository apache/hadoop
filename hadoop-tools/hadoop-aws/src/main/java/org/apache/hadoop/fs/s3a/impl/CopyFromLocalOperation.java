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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections4.comparators.ReverseComparator;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;

import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.store.audit.AuditingFunctions.callableWithinAuditSpan;

/**
 * Implementation of CopyFromLocalOperation.
 * <p>
 * This operation copies a file or directory (recursively) from a local
 * FS to an object store. Initially, this operation has been developed for
 * S3 (s3a) interaction, however, there's minimal work needed for it to
 * work with other stores.
 * </p>
 * <p>How the uploading of files works:</p>
 * <ul>
 *     <li> all source files and directories are scanned through;</li>
 *     <li> the LARGEST_N_FILES start uploading; </li>
 *     <li> the remaining files are shuffled and uploaded; </li>
 *     <li>
 *         any remaining empty directory is uploaded too to preserve local
 *         tree structure.
 *     </li>
 * </ul>
 */
public class CopyFromLocalOperation extends ExecutingStoreOperation<Void> {

  /**
   * Largest N files to be uploaded first.
   */
  private static final int LARGEST_N_FILES = 5;

  private static final Logger LOG = LoggerFactory.getLogger(
      CopyFromLocalOperation.class);

  /**
   * Callbacks to be used by this operation for external / IO actions.
   */
  private final CopyFromLocalOperationCallbacks callbacks;

  /**
   * Delete source after operation finishes.
   */
  private final boolean deleteSource;

  /**
   * Overwrite destination files / folders.
   */
  private final boolean overwrite;

  /**
   * Source path to file / directory.
   */
  private final Path source;

  /**
   * Async operations executor.
   */
  private final ListeningExecutorService executor;

  /**
   * Destination path.
   */
  private Path destination;

  /**
   * Destination file status.
   */
  private FileStatus destStatus;

  public CopyFromLocalOperation(
      final StoreContext storeContext,
      Path source,
      Path destination,
      boolean deleteSource,
      boolean overwrite,
      CopyFromLocalOperationCallbacks callbacks) {
    super(storeContext);
    this.callbacks = callbacks;
    this.deleteSource = deleteSource;
    this.overwrite = overwrite;
    this.source = source.toUri().getScheme() == null ? new Path("file://", source) : source;
    this.destination = destination;

    // Capacity of 1 is a safe default for now since transfer manager can also
    // spawn threads when uploading bigger files.
    this.executor = MoreExecutors.listeningDecorator(
        storeContext.createThrottledExecutor(1)
    );
  }

  /**
   * Executes the {@link CopyFromLocalOperation}.
   *
   * @throws IOException         - if there are any failures with upload or deletion
   *                             of files. Check {@link CopyFromLocalOperationCallbacks} for specifics.
   * @throws PathExistsException - if the path exists and no overwrite flag
   *                             is set OR if the source is file and destination is a directory
   */
  @Override
  @Retries.RetryTranslated
  public Void execute()
      throws IOException, PathExistsException {
    LOG.debug("Copying local file from {} to {}", source, destination);
    File sourceFile = callbacks.pathToLocalFile(source);
    updateDestStatus(destination);

    // Handles bar/ -> foo/ => foo/bar and bar/ -> foo/bar/ => foo/bar/bar
    if (getDestStatus().isPresent() && getDestStatus().get().isDirectory()
        && sourceFile.isDirectory()) {
      destination = new Path(destination, sourceFile.getName());
      LOG.debug("Destination updated to: {}", destination);
      updateDestStatus(destination);
    }

    checkSource(sourceFile);
    checkDestination(destination, sourceFile, overwrite);
    uploadSourceFromFS();

    if (deleteSource) {
      callbacks.deleteLocal(source, true);
    }

    return null;
  }

  /**
   * Does a {@link CopyFromLocalOperationCallbacks#getFileStatus(Path)}
   * operation on the provided destination and updates the internal status of
   * destStatus field.
   *
   * @param  dest - destination Path
   * @throws IOException if getFileStatus fails
   */
  private void updateDestStatus(Path dest) throws IOException {
    try {
      destStatus = callbacks.getFileStatus(dest);
    } catch (FileNotFoundException e) {
      destStatus = null;
    }
  }

  /**
   * Starts async upload operations for files. Creating an empty directory
   * classifies as a "file upload".
   *
   * Check {@link CopyFromLocalOperation} for details on the order of
   * operations.
   *
   * @throws IOException - if listing or upload fail
   */
  private void uploadSourceFromFS() throws IOException {
    RemoteIterator<LocatedFileStatus> localFiles = listFilesAndDirs(source);
    List<CompletableFuture<Void>> activeOps = new ArrayList<>();

    // After all files are traversed, this set will contain only emptyDirs
    Set<Path> emptyDirs = new HashSet<>();
    List<UploadEntry> entries = new ArrayList<>();
    while (localFiles.hasNext()) {
      LocatedFileStatus sourceFile = localFiles.next();
      Path sourceFilePath = sourceFile.getPath();

      // Directory containing this file / directory isn't empty
      emptyDirs.remove(sourceFilePath.getParent());

      if (sourceFile.isDirectory()) {
        emptyDirs.add(sourceFilePath);
        continue;
      }

      Path destPath = getFinalPath(sourceFilePath);
      // UploadEntries: have a destination path, a file size
      entries.add(new UploadEntry(
          sourceFilePath,
          destPath,
          sourceFile.getLen()));
    }

    if (localFiles instanceof Closeable) {
      IOUtils.closeStream((Closeable) localFiles);
    }

    // Sort all upload entries based on size
    entries.sort(new ReverseComparator(new UploadEntry.SizeComparator()));

    // Take only top most N entries and upload
    final int sortedUploadsCount = Math.min(LARGEST_N_FILES, entries.size());
    List<UploadEntry> markedForUpload = new ArrayList<>();

    for (int uploadNo = 0; uploadNo < sortedUploadsCount; uploadNo++) {
      UploadEntry uploadEntry = entries.get(uploadNo);
      File file = callbacks.pathToLocalFile(uploadEntry.source);
      activeOps.add(submitUpload(file, uploadEntry));
      markedForUpload.add(uploadEntry);
    }

    // No files found, it's empty source directory
    if (entries.isEmpty()) {
      emptyDirs.add(source);
    }

    // Shuffle all remaining entries and upload them
    entries.removeAll(markedForUpload);
    Collections.shuffle(entries);
    for (UploadEntry uploadEntry : entries) {
      File file = callbacks.pathToLocalFile(uploadEntry.source);
      activeOps.add(submitUpload(file, uploadEntry));
    }

    for (Path emptyDir : emptyDirs) {
      Path emptyDirPath = getFinalPath(emptyDir);
      activeOps.add(submitCreateEmptyDir(emptyDirPath));
    }

    waitForCompletion(activeOps);
  }

  /**
   * Async call to create an empty directory.
   *
   * @param dir directory path
   * @return the submitted future
   */
  private CompletableFuture<Void> submitCreateEmptyDir(Path dir) {
    return submit(executor, callableWithinAuditSpan(
        getAuditSpan(), () -> {
          callbacks.createEmptyDir(dir, getStoreContext());
          return null;
        }
    ));
  }

  /**
   * Async call to upload a file.
   *
   * @param file        - File to be uploaded
   * @param uploadEntry - Upload entry holding the source and destination
   * @return the submitted future
   */
  private CompletableFuture<Void> submitUpload(
      File file,
      UploadEntry uploadEntry) {
    return submit(executor, callableWithinAuditSpan(
        getAuditSpan(), () -> {
          callbacks.copyLocalFileFromTo(
              file,
              uploadEntry.source,
              uploadEntry.destination);
          return null;
        }
    ));
  }

  /**
   * Checks the source before upload starts.
   *
   * @param src - Source file
   * @throws FileNotFoundException - if the file isn't found
   */
  private void checkSource(File src)
      throws FileNotFoundException {
    if (!src.exists()) {
      throw new FileNotFoundException("No file: " + src.getPath());
    }
  }

  /**
   * Check the destination path and make sure it's compatible with the source,
   * i.e. source and destination are both files / directories.
   *
   * @param dest      - Destination path
   * @param src       - Source file
   * @param overwrite - Should source overwrite destination
   * @throws PathExistsException - If the destination path exists and no
   *                             overwrite flag is set
   * @throws FileAlreadyExistsException - If source is file and destination is path
   */
  private void checkDestination(
      Path dest,
      File src,
      boolean overwrite) throws PathExistsException,
      FileAlreadyExistsException {
    if (!getDestStatus().isPresent()) {
      return;
    }

    if (src.isDirectory() && getDestStatus().get().isFile()) {
      throw new FileAlreadyExistsException(
          "Source '" + src.getPath() + "' is directory and " +
              "destination '" + dest + "' is file");
    }

    if (!overwrite) {
      throw new PathExistsException(dest + " already exists");
    }
  }

  /**
   * Get the final path of a source file with regards to its destination.
   *
   * @param src - source path
   * @return - the final path for the source file to be uploaded to
   * @throws PathIOException - if a relative path can't be created
   */
  private Path getFinalPath(Path src) throws PathIOException {
    URI currentSrcUri = src.toUri();
    URI relativeSrcUri = source.toUri().relativize(currentSrcUri);
    if (relativeSrcUri.equals(currentSrcUri)) {
      throw new PathIOException("Cannot get relative path for URI:"
          + relativeSrcUri);
    }

    Optional<FileStatus> status = getDestStatus();
    if (!relativeSrcUri.getPath().isEmpty()) {
      return new Path(destination, relativeSrcUri.getPath());
    } else if (status.isPresent() && status.get().isDirectory()) {
      // file to dir
      return new Path(destination, src.getName());
    } else {
      // file to file
      return destination;
    }
  }

  private Optional<FileStatus> getDestStatus() {
    return Optional.ofNullable(destStatus);
  }

  /**
   * {@link RemoteIterator} which lists all of the files and directories for
   * a given path. It's strikingly similar to
   * {@link org.apache.hadoop.fs.LocalFileSystem#listFiles(Path, boolean)}
   * however with the small addition that it includes directories.
   *
   * @param path - Path to list files and directories from
   * @return - an iterator
   * @throws IOException - if listing of a path file fails
   */
  private RemoteIterator<LocatedFileStatus> listFilesAndDirs(Path path)
      throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private final Stack<RemoteIterator<LocatedFileStatus>> iterators =
          new Stack<>();
      private RemoteIterator<LocatedFileStatus> current =
          callbacks.listLocalStatusIterator(path);
      private LocatedFileStatus curFile;

      @Override
      public boolean hasNext() throws IOException {
        while (curFile == null) {
          if (current.hasNext()) {
            handleFileStat(current.next());
          } else if (!iterators.empty()) {
            current = iterators.pop();
          } else {
            return false;
          }
        }
        return true;
      }

      /**
       * Process the input stat.
       * If it is a file or directory return the file stat.
       * If it is a directory, traverse the directory;
       * @param stat input status
       * @throws IOException if any IO error occurs
       */
      private void handleFileStat(LocatedFileStatus stat)
          throws IOException {
        if (stat.isFile()) { // file
          curFile = stat;
        } else { // directory
          curFile = stat;
          iterators.push(current);
          current = callbacks.listLocalStatusIterator(stat.getPath());
        }
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          LocatedFileStatus result = curFile;
          curFile = null;
          return result;
        }
        throw new NoSuchElementException("No more entry in "
            + path);
      }
    };
  }

  /**
   * <p>Represents an entry for a file to be moved.</p>
   * <p>
   * Helpful with sorting files by their size and keeping track of path
   * information for the upload.
   * </p>
   */
  private static final class UploadEntry {
    private final Path source;
    private final Path destination;
    private final long size;

    private UploadEntry(Path source, Path destination, long size) {
      this.source = source;
      this.destination = destination;
      this.size = size;
    }

    /**
     * Compares {@link UploadEntry} objects and produces DESC ordering.
     */
    static class SizeComparator implements Comparator<UploadEntry>,
        Serializable {
      @Override
      public int compare(UploadEntry entry1, UploadEntry entry2) {
        return Long.compare(entry1.size, entry2.size);
      }
    }
  }

  /**
   * Define the contract for {@link CopyFromLocalOperation} to interact
   * with any external resources.
   */
  public interface CopyFromLocalOperationCallbacks {
    /**
     * List all entries (files AND directories) for a path.
     *
     * @param path - path to list
     * @return an iterator for all entries
     * @throws IOException - for any failure
     */
    RemoteIterator<LocatedFileStatus> listLocalStatusIterator(Path path)
        throws IOException;

    /**
     * Get the file status for a path.
     *
     * @param path - target path
     * @return FileStatus
     * @throws IOException - for any failure
     */
    FileStatus getFileStatus(Path path) throws IOException;

    /**
     * Get the file from a path.
     *
     * @param path - target path
     * @return file at path
     */
    File pathToLocalFile(Path path);

    /**
     * Delete file / directory at path.
     *
     * @param path      - target path
     * @param recursive - recursive deletion
     * @return boolean result of operation
     * @throws IOException for any failure
     */
    boolean deleteLocal(Path path, boolean recursive) throws IOException;

    /**
     * Copy / Upload a file from a source path to a destination path.
     *
     * @param file        - target file
     * @param source      - source path
     * @param destination - destination path
     * @throws IOException for any failure
     */
    void copyLocalFileFromTo(
        File file,
        Path source,
        Path destination) throws IOException;

    /**
     * Create empty directory at path. Most likely an upload operation.
     *
     * @param path - target path
     * @param storeContext - store context
     * @return boolean result of operation
     * @throws IOException for any failure
     */
    boolean createEmptyDir(Path path, StoreContext storeContext)
        throws IOException;
  }
}
