package org.apache.hadoop.fs.azurebfs.services;

import java.util.LinkedList;
import java.util.Queue;

public class ProcessingQueue<T> {

    private final Queue<T> internalQueue = new LinkedList();
    private int processorCount = 0;

    ProcessingQueue() {
    }

    public synchronized void add(T item) {
      if (item == null) {
        throw new IllegalArgumentException("Cannot put null into queue");
      } else {
        this.internalQueue.add(item);
        this.notifyAll();
      }
    }

    public synchronized T poll() {
      while(true) {
        try {
          if (this.isQueueEmpty() && !this.done()) {
            this.wait();
            continue;
          }

          if (!this.isQueueEmpty()) {
            ++this.processorCount;
            return this.internalQueue.poll();
          }

          if (this.done()) {
            return null;
          }
        } catch (InterruptedException var2) {
          Thread.currentThread().interrupt();
        }

        return null;
      }
    }

    public synchronized void unregister() {
      --this.processorCount;
      if (this.processorCount < 0) {
        throw new IllegalStateException("too many unregister()'s. processorCount is now " + this.processorCount);
      } else {
        if (this.done()) {
          this.notifyAll();
        }

      }
    }

    private boolean done() {
      return this.processorCount == 0 && this.isQueueEmpty();
    }

    private boolean isQueueEmpty() {
      return this.internalQueue.peek() == null;
    }
}
