package org.apache.hadoop.hbase.regionserver;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the read/write consistency within memstore. This provides
 * an interface for readers to determine what entries to ignore, and
 * a mechanism for writers to obtain new write numbers, then "commit"
 * the new writes for readers to read (thus forming atomic transactions).
 */
public class ReadWriteConsistencyControl {
  private final AtomicLong memstoreRead = new AtomicLong();
  private final AtomicLong memstoreWrite = new AtomicLong();
  // This is the pending queue of writes.
  private final LinkedList<WriteEntry> writeQueue =
      new LinkedList<WriteEntry>();

  private static final ThreadLocal<Long> perThreadReadPoint =
      new ThreadLocal<Long>();

  public static long getThreadReadPoint() {
    return perThreadReadPoint.get();
  }

  public static void setThreadReadPoint(long readPoint) {
    perThreadReadPoint.set(readPoint);
  }

  public static long resetThreadReadPoint(ReadWriteConsistencyControl rwcc) {
    perThreadReadPoint.set(rwcc.memstoreReadPoint());
    return getThreadReadPoint();
  }

  public WriteEntry beginMemstoreInsert() {
    synchronized (writeQueue) {
      long nextWriteNumber = memstoreWrite.incrementAndGet();
      WriteEntry e = new WriteEntry(nextWriteNumber);
      writeQueue.add(e);
      return e;
    }
  }
  public void completeMemstoreInsert(WriteEntry e) {
    synchronized (writeQueue) {
      e.markCompleted();

      long nextReadValue = -1;
      boolean ranOnce=false;
      while (!writeQueue.isEmpty()) {
        ranOnce=true;
        WriteEntry queueFirst = writeQueue.getFirst();

        if (nextReadValue > 0) {
          if (nextReadValue+1 != queueFirst.getWriteNumber()) {
            throw new RuntimeException("invariant in completeMemstoreInsert violated, prev: "
                + nextReadValue + " next: " + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          writeQueue.removeFirst();
        } else {
          break;
        }
      }

      if (!ranOnce) {
        throw new RuntimeException("never was a first");
      }

      if (nextReadValue > 0) {
        memstoreRead.set(nextReadValue);
      }
    }

    // Spin until any other concurrent puts have finished. This makes sure that
    // if we move on to construct a scanner, we'll get read-your-own-writes
    // consistency. We anticipate that since puts to the memstore are very fast,
    // this will be on the order of microseconds - so spinning should be faster
    // than a condition variable.
    int spun = 0;
    while (memstoreRead.get() < e.getWriteNumber()) {
      spun++;
    }
    // Could potentially expose spun as a metric
  }

  public long memstoreReadPoint() {
    return memstoreRead.get();
  }


  public static class WriteEntry {
    private long writeNumber;
    private boolean completed = false;
    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }
    void markCompleted() {
      this.completed = true;
    }
    boolean isCompleted() {
      return this.completed;
    }
    long getWriteNumber() {
      return this.writeNumber;
    }
  }
}
