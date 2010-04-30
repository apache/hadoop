package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Keeps up the queue of {@link RegionServerOperation}s.
 * Has both live queue and a temporary put-aside queue; if processing of the
 * live todo queue fails for some reason, we'll add the item back on the delay
 * queue for retry later.  Call {@link #shutdown()} to effect a cleanup of
 * queues when done.  Listen to this queue by registering
 * {@link RegionServerOperationListener}s.
 * @see #registerRegionServerOperationListener(RegionServerOperationListener)
 * @see #unregisterRegionServerOperationListener(RegionServerOperationListener)
 */
public class RegionServerOperationQueue {
  // TODO: Build up the junit test of this class.
  private final Log LOG = LogFactory.getLog(this.getClass());
  
  /**
   * Enums returned by {@link RegionServerOperationQueue#process()};
   */
  public static enum ProcessingResultCode {
    /**
     * Operation was processed successfully.
     */
    PROCESSED,
    /**
     * Nothing to do.
     */
    NOOP,
    /**
     * Operation was put-aside for now.  Will be retried later.
     */
    REQUEUED,
    /**
     * Failed processing of the operation.
     */
    FAILED,
    /**
     * Operation was requeued but we failed its processing for some reason
     * (Bad filesystem?).
     */
    REQUEUED_BUT_PROBLEM
  };

  /*
   * Do not put items directly on this queue. Use {@link #putOnDelayQueue(RegionServerOperation)}.
   * It makes sure the expiration on the RegionServerOperation added is updated.
   */
  private final DelayQueue<RegionServerOperation> delayedToDoQueue =
    new DelayQueue<RegionServerOperation>();
  private final BlockingQueue<RegionServerOperation> toDoQueue =
    new PriorityBlockingQueue<RegionServerOperation>();
  private final Set<RegionServerOperationListener> listeners =
    new CopyOnWriteArraySet<RegionServerOperationListener>();
  private final int threadWakeFrequency;
  private final AtomicBoolean closed;
  private final Sleeper sleeper;

  RegionServerOperationQueue(final Configuration c, final AtomicBoolean closed) {
    this.threadWakeFrequency = c.getInt(HMaster.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.closed = closed;
    this.sleeper = new Sleeper(this.threadWakeFrequency, this.closed);
  }

  public void put(final RegionServerOperation op) {
    try {
      this.toDoQueue.put(op);
    } catch (InterruptedException e) {
      LOG.warn("Insertion into todo queue interrupted; putting on delay queue", e);
      putOnDelayQueue(op);
    }
  }

  /**
   * Try to get an operation off of the queue and process it.
   * @return {@link ProcessingResultCode#PROCESSED},
   * {@link ProcessingResultCode#REQUEUED},
   * {@link ProcessingResultCode#REQUEUED_BUT_PROBLEM}
   */ 
  public synchronized ProcessingResultCode process() {
    RegionServerOperation op = delayedToDoQueue.poll();
    // if there aren't any todo items in the queue, sleep for a bit.
    if (op == null) {
      try {
        op = toDoQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted", e);
      }
    }

    // At this point, if there's still no todo operation, or we're supposed to
    // be closed, return.
    if (op == null || closed.get()) {
      return ProcessingResultCode.NOOP;
    }

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing todo: " + op.toString());
      }
      if (!process(op)) {
        // Add it back on the queue.
        putOnDelayQueue(op);
      } else if (op.process()) {
        processed(op);
      } else {
        // Operation would have blocked because not all meta regions are
        // online. This could cause a deadlock, because this thread is waiting
        // for the missing meta region(s) to come back online, but since it
        // is waiting, it cannot process the meta region online operation it
        // is waiting for. So put this operation back on the queue for now.
        if (toDoQueue.size() == 0) {
          // The queue is currently empty so wait for a while to see if what
          // we need comes in first
          this.sleeper.sleep();
        }
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Put " + op.toString() + " back on queue");
          }
          toDoQueue.put(op);
        } catch (InterruptedException e) {
          throw new RuntimeException(
            "Putting into toDoQueue was interrupted.", e);
        }
      }
    } catch (Exception ex) {
      // There was an exception performing the operation.
      if (ex instanceof RemoteException) {
        try {
          ex = RemoteExceptionHandler.decodeRemoteException(
            (RemoteException)ex);
        } catch (IOException e) {
          ex = e;
          LOG.warn("main processing loop: " + op.toString(), e);
        }
      }
      LOG.warn("Failed processing: " + op.toString() +
        "; putting onto delayed todo queue", ex);
      putOnDelayQueue(op);
      return ProcessingResultCode.REQUEUED_BUT_PROBLEM;
    }
    return ProcessingResultCode.REQUEUED;
  }

  void putOnDelayQueue(final RegionServerOperation op) {
    op.resetExpiration();
    this.delayedToDoQueue.put(op);
  }

  /**
   * Clean up the queues.
   */
  public synchronized void shutdown() {
    this.toDoQueue.clear();
    this.delayedToDoQueue.clear();
  }

  /**
   * @param l Register this listener of RegionServerOperation events.
   */
  public void registerRegionServerOperationListener(final RegionServerOperationListener l) {
    this.listeners.add(l);
  }

  /**
   * @param l Unregister this listener for RegionServerOperation events.
   * @return True if this listener was registered.
   */
  public boolean unregisterRegionServerOperationListener(final RegionServerOperationListener l) {
    return this.listeners.remove(l);
  }

  /*
   * Tell listeners that we processed a RegionServerOperation.
   * @param op Operation to tell the world about.
   */
  private void processed(final RegionServerOperation op) {
    if (this.listeners.isEmpty()) return;
    for (RegionServerOperationListener listener: this.listeners) {
      listener.processed(op);
    }
  }

  /*
   * Tell listeners that we processed a RegionServerOperation.
   * @param op Operation to tell the world about.
   */
  private boolean process(final RegionServerOperation op) throws IOException {
    if (this.listeners.isEmpty()) return true;
    for (RegionServerOperationListener listener: this.listeners) {
      if (!listener.process(op)) return false;
    }
    return true;
  }
}
