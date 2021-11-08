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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Cli.*;

/** For validating {@link INodeReference} subclasses. */
public class INodeReferenceValidation {
  public static final Logger LOG = LoggerFactory.getLogger(
      INodeReferenceValidation.class);

  private static final AtomicReference<INodeReferenceValidation> INSTANCE
      = new AtomicReference<>();

  public static void start() {
    INSTANCE.compareAndSet(null, new INodeReferenceValidation());
    println("%s started", INodeReferenceValidation.class.getSimpleName());
  }

  public static void end(AtomicInteger errorCount) {
    final INodeReferenceValidation instance = INSTANCE.getAndSet(null);
    if (instance == null) {
      return;
    }

    final int initCount = errorCount.get();
    instance.assertReferences(errorCount);
    println("%s ended successfully: %d error(s) found.",
        INodeReferenceValidation.class.getSimpleName(),
        errorCount.get() - initCount);
  }

  static <REF extends INodeReference> void add(REF ref, Class<REF> clazz) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      final boolean added = validation.getReferences(clazz).add(ref);
      Preconditions.checkState(added);
      LOG.trace("add {}: {}", clazz, ref.toDetailString());
    }
  }

  static <REF extends INodeReference> void remove(REF ref, Class<REF> clazz) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      final boolean removed = validation.getReferences(clazz).remove(ref);
      Preconditions.checkState(removed);
      LOG.trace("remove {}: {}", clazz, ref.toDetailString());
    }
  }

  static class ReferenceSet<REF extends INodeReference> {
    private final Class<REF> clazz;
    private final List<REF> references = new LinkedList<>();
    private volatile List<Task<REF>> tasks;
    private volatile List<Future<Integer>> futures;
    private final AtomicInteger taskCompleted = new AtomicInteger();

    ReferenceSet(Class<REF> clazz) {
      this.clazz = clazz;
    }

    boolean add(REF ref) {
      return references.add(ref);
    }

    boolean remove(REF ref) {
      for(final Iterator<REF> i = references.iterator(); i.hasNext();) {
        if (i.next() == ref) {
          i.remove();
          return true;
        }
      }
      return false;
    }

    void submit(AtomicInteger errorCount, ExecutorService service)
        throws InterruptedException {
      final int size = references.size();
      tasks = createTasks(references, errorCount);
      println("Submitting %d tasks for validating %s %s(s)",
          tasks.size(), Util.toCommaSeparatedNumber(size),
          clazz.getSimpleName());
      futures = service.invokeAll(tasks);
    }

    void waitForFutures() throws Exception {
      for(Future<Integer> f : futures) {
        f.get();
        taskCompleted.incrementAndGet();
      }
    }

    double getTaskCompletedPercent() {
      final List<Task<REF>> t = tasks;
      return t == null? 0
          : t.isEmpty()? 100
          : taskCompleted.get()*100.0/tasks.size();
    }

    @Override
    public String toString() {
      return String.format("%s %.1f%%",
          clazz.getSimpleName(), getTaskCompletedPercent());
    }
  }

  private final ReferenceSet<INodeReference.WithCount> withCounts
      = new ReferenceSet<>(INodeReference.WithCount.class);
  private final ReferenceSet<INodeReference.WithName> withNames
      = new ReferenceSet<>(INodeReference.WithName.class);
  private final ReferenceSet<INodeReference.DstReference> dstReferences
      = new ReferenceSet<>(INodeReference.DstReference.class);

  <REF extends INodeReference> ReferenceSet<REF> getReferences(
      Class<REF> clazz) {
    if (clazz == INodeReference.WithCount.class) {
      return (ReferenceSet<REF>) withCounts;
    } else if (clazz == INodeReference.WithName.class) {
      return (ReferenceSet<REF>) withNames;
    } else if (clazz == INodeReference.DstReference.class) {
      return (ReferenceSet<REF>) dstReferences;
    }
    throw new IllegalArgumentException("References not found for " + clazz);
  }

  private void assertReferences(AtomicInteger errorCount) {
    final int p = Runtime.getRuntime().availableProcessors();
    LOG.info("Available Processors: {}", p);
    final ExecutorService service = Executors.newFixedThreadPool(p);

    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        LOG.info("ASSERT_REFERENCES Progress: {}, {}, {}",
            dstReferences, withCounts, withNames);
      }
    };
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 1_000);

    try {
      dstReferences.submit(errorCount, service);
      withCounts.submit(errorCount, service);
      withNames.submit(errorCount, service);

      dstReferences.waitForFutures();
      withCounts.waitForFutures();
      withNames.waitForFutures();
    } catch (Throwable e) {
      printError("Failed to assertReferences", e);
    } finally {
      service.shutdown();
      t.cancel();
    }
  }

  static <REF extends INodeReference> List<Task<REF>> createTasks(
      List<REF> references, AtomicInteger errorCount) {
    final List<Task<REF>> tasks = new LinkedList<>();
    for (final Iterator<REF> i = references.iterator(); i.hasNext();) {
      tasks.add(new Task<>(i, errorCount));
    }
    return tasks;
  }

  static class Task<REF extends INodeReference> implements Callable<Integer> {
    static final int BATCH_SIZE = 100_000;

    private final List<REF> references = new LinkedList<>();
    private final AtomicInteger errorCount;

    Task(Iterator<REF> i, AtomicInteger errorCount) {
      for(int n = 0; i.hasNext() && n < BATCH_SIZE; n++) {
        references.add(i.next());
        i.remove();
      }
      this.errorCount = errorCount;
    }

    @Override
    public Integer call() throws Exception {
      for (final REF ref : references) {
        try {
          ref.assertReferences();
        } catch (Throwable t) {
          printError(errorCount, "%s", t);
        }
      }
      return references.size();
    }
  }
}