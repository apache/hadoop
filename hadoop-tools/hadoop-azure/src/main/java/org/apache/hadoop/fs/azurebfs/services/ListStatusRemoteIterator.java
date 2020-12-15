package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ListStatusRemoteIterator implements RemoteIterator<FileStatus> {

  private static final boolean FETCH_ALL_FALSE = false;

  private final Path path;
  private final AzureBlobFileSystemStore abfsStore;
  private final List<List<FileStatus>> allFileStatuses;

  private int currentRow;
  private int currentColumn;
  private String continuation;
  private CompletableFuture<IOException> future;
  private boolean isFirstRead = true;

  public ListStatusRemoteIterator(final Path path,
      final AzureBlobFileSystemStore abfsStore) throws IOException {
    this.path = path;
    this.abfsStore = abfsStore;
    this.allFileStatuses = new ArrayList<>();
    getMoreFileStatuses();
    getMoreFileStatuses();
    forceCurrentFuture();
    currentRow = 0;
    currentColumn = 0;
  }

  @Override
  public boolean hasNext() throws IOException {
    return currentRow < getRowCount() && currentColumn < getColCount();
  }

  @Override
  public FileStatus next() throws IOException {
    if (currentRow >= getRowCount()
        || (currentColumn >= getColCount()) && currentRow >= getRowCount()) {
      throw new NoSuchElementException();
    }
    FileStatus fs = getCurrentElement();
    updatePointers();
    return fs;
  }

  private void updatePointers() throws IOException {
    currentColumn++;
    if (currentColumn >= getColCount()) {
      currentColumn = 0;
      currentRow++;
      getMoreFileStatuses();
    }
  }

  private int getRowCount() {
    return this.allFileStatuses.size();
  }

  private FileStatus getCurrentElement() {
    return getCurrentRow().get(currentColumn);
  }

  private int getColCount() {
    return getCurrentRow().size();
  }

  private List<FileStatus> getCurrentRow() {
    return this.allFileStatuses.get(currentRow);
  }

  private void forceCurrentFuture() throws IOException {
    if (future == null) {
      return;
    }
    IOException ex;
    try {
      ex = future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
    if (ex != null) {
      throw ex;
    }
    future = null;
  }

  private void getMoreFileStatuses() throws IOException {
    forceCurrentFuture();
    future = CompletableFuture.supplyAsync(() -> {
      try {
        if (!isFirstRead && (continuation == null || continuation.isEmpty())) {
          return null;
        }
        List<FileStatus> fileStatuses = new ArrayList<>();
        continuation = abfsStore
            .listStatus(path, null, fileStatuses, FETCH_ALL_FALSE,
                continuation);
        isFirstRead = false;
        if (!fileStatuses.isEmpty()) {
          this.allFileStatuses.add(fileStatuses);
        }
        return null;
      } catch (IOException e) {
        return e;
      }
    });
  }

}
