package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;

public class ListStatusRemoteIterator implements RemoteIterator<FileStatus> {

  private static final boolean FETCH_ALL_FALSE = false;

  private final Path path;
  private final AzureBlobFileSystemStore abfsStore;
  private final Queue<ListIterator<FileStatus>> iterators = new LinkedList<>();

  private boolean firstRead = true;
  private CompletableFuture<IOException> future;
  private String continuation;
  private ListIterator<FileStatus> lsItr;

  public ListStatusRemoteIterator(final Path path,
      final AzureBlobFileSystemStore abfsStore) throws IOException {
    this.path = path;
    this.abfsStore = abfsStore;
    fetchMoreFileStatusesAsync();
    fetchMoreFileStatusesAsync();
    lsItr = iterators.poll();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (lsItr.hasNext()) {
      return true;
    }
    fetchMoreFileStatusesAsync();
    if (!iterators.isEmpty()) {
      lsItr = iterators.poll();
      if (lsItr == null) {
        return false;
      }
    }
    return lsItr.hasNext();
  }

  @Override
  public FileStatus next() throws IOException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }
    return lsItr.next();
  }

  private void fetchMoreFileStatusesAsync() throws IOException {
    forceFuture();
    if (isIterationComplete()) {
      return;
    }
    this.future = CompletableFuture.supplyAsync(() -> {
      try {
        List<FileStatus> fileStatuses = new ArrayList<>();
        continuation = abfsStore
            .listStatus(path, null, fileStatuses, FETCH_ALL_FALSE,
                continuation);
        if (fileStatuses != null && !fileStatuses.isEmpty()) {
          iterators.add(fileStatuses.listIterator());
        }
        if (firstRead) {
          firstRead = false;
        }
        return null;
      } catch (IOException ex) {
        return ex;
      }
    });
  }

  private void forceFuture() throws IOException {
    if (future == null) {
      return;
    }
    IOException ex = awaitFuture(future);
    if (ex != null) {
      throw ex;
    }
  }

  private boolean isIterationComplete() {
    return !firstRead && (continuation == null || continuation.isEmpty());
  }

}
