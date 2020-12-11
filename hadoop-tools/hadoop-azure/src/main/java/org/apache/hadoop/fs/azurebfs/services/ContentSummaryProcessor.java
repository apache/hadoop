package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.ContentSummary;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ContentSummaryProcessor {
  private AtomicLong fileCount = new AtomicLong(0L);
  private AtomicLong directoryCount = new AtomicLong(0L);
  private AtomicLong totalBytes = new AtomicLong(0L);
  private ProcessingQueue<FileStatus> queue = new ProcessingQueue();
  private AzureBlobFileSystemStore abfsStore;
  private static final int NUM_THREADS = 16;

  public ContentSummaryProcessor() {
  }

  public ContentSummary getContentSummary(AzureBlobFileSystemStore abfsStore,
      Path path) throws IOException {
    this.abfsStore = abfsStore;
    FileStatus fileStatus = abfsStore.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      processFile(fileStatus);
    } else {
      this.queue.add(fileStatus);
      Thread[] threads = new Thread[16];

      for(int i = 0; i < NUM_THREADS; ++i) {
        threads[i] = new Thread(new ContentSummaryProcessor.ThreadProcessor());
        threads[i].start();
      }

      for (Thread t : threads) {
        try {
          t.join();
        } catch (InterruptedException var10) {
          Thread.currentThread().interrupt();
        }
      }
    }

    return new ContentSummary(totalBytes.get(), directoryCount.get(), fileCount.get(), totalBytes.get());
  }

  private void processDirectoryTree(Path path) throws IOException {
    FileStatus[] fileStatuses = enumerateDirectoryInternal(path.toString());
    if (fileStatuses == null || fileStatuses.length == 0) {
      return;
    }
    for (FileStatus fileStatus : fileStatuses) {
      System.out.println(fileStatus.getPath().toString());
      if (fileStatus.isDirectory()) {
        this.queue.add(fileStatus);
        this.processDirectory();
        System.out.println("===== incrementing dir count!!!");
      } else {
        this.processFile(fileStatus);
      }
    }
  }

  private void processDirectory() {
    this.directoryCount.incrementAndGet();
    System.out.println("----- dir count is " + directoryCount);
  }

  private void processFile(FileStatus fileStatus) {
    this.fileCount.incrementAndGet();
    this.totalBytes.addAndGet(fileStatus.getLen());
  }

  private FileStatus[] enumerateDirectoryInternal(String path) throws IOException {
    return abfsStore.listStatus(new Path(path)); //try catch if needed
  }

  private class ThreadProcessor implements Runnable {
    private ThreadProcessor() {
    }

    public void run() {
      try {
        FileStatus fileStatus;
        while((fileStatus = ContentSummaryProcessor.this.queue.poll()) != null) {
          if (fileStatus.isDirectory()) {
            ContentSummaryProcessor.this.processDirectoryTree(fileStatus.getPath());
          }
          ContentSummaryProcessor.this.queue.unregister();
        }

      } catch (IOException var9) {
        throw new RuntimeException("IOException processing Directory tree", var9);
      }
    }
  }
}
