/**
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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract class MergeThread<T,K,V> extends Thread {
  
  private static final Log LOG = LogFactory.getLog(MergeThread.class);

  private volatile boolean inProgress = false;
  private List<T> inputs = new ArrayList<T>();
  protected final MergeManager<K,V> manager;
  private final ExceptionReporter reporter;
  private boolean closed = false;
  private final int mergeFactor;
  
  public MergeThread(MergeManager<K,V> manager, int mergeFactor,
                     ExceptionReporter reporter) {
    this.manager = manager;
    this.mergeFactor = mergeFactor;
    this.reporter = reporter;
  }
  
  public synchronized void close() throws InterruptedException {
    closed = true;
    waitForMerge();
    interrupt();
  }

  public synchronized boolean isInProgress() {
    return inProgress;
  }
  
  public synchronized void startMerge(Set<T> inputs) {
    if (!closed) {
      inProgress = true;
      this.inputs = new ArrayList<T>();
      Iterator<T> iter=inputs.iterator();
      for (int ctr = 0; iter.hasNext() && ctr < mergeFactor; ++ctr) {
        this.inputs.add(iter.next());
        iter.remove();
      }
      LOG.info(getName() + ": Starting merge with " + this.inputs.size() + 
               " segments, while ignoring " + inputs.size() + " segments");
      notifyAll();
    }
  }

  public synchronized void waitForMerge() throws InterruptedException {
    while (inProgress) {
      wait();
    }
  }

  public void run() {
    while (true) {
      try {
        // Wait for notification to start the merge...
        synchronized (this) {
          while (!inProgress) {
            wait();
          }
        }

        // Merge
        merge(inputs);
      } catch (InterruptedException ie) {
        return;
      } catch(Throwable t) {
        reporter.reportException(t);
        return;
      } finally {
        synchronized (this) {
          // Clear inputs
          inputs = null;
          inProgress = false;        
          notifyAll();
        }
      }
    }
  }

  public abstract void merge(List<T> inputs) throws IOException;
}
