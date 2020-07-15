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

import static org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Cli.printError;
import static org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Cli.println;

/** For validating {@link INodeReference} subclasses. */
public class INodeReferenceValidation {
  public static final Logger LOG = LoggerFactory.getLogger(
      INodeReferenceValidation.class);

  private static final AtomicReference<INodeReferenceValidation> INSTANCE
      = new AtomicReference<>();

  public static void start() {
    INSTANCE.compareAndSet(null, new INodeReferenceValidation());
    println("Validation started");
  }

  public static int end() {
    final INodeReferenceValidation instance = INSTANCE.getAndSet(null);
    if (instance == null) {
      return 0;
    }

    final int errorCount = instance.assertReferences();
    println("Validation ended successfully: %d error(s) found.", errorCount);
    return errorCount;
  }

  static void addWithCount(INodeReference.WithCount c) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      validation.withCounts.add(c);
      if (LOG.isTraceEnabled()) {
        LOG.trace("addWithCount: " + c.toDetailString());
      }
    }
  }

  static void addWithName(INodeReference.WithName n) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      validation.withNames.add(n);
      if (LOG.isTraceEnabled()) {
        LOG.trace("addWithName: {}", n.toDetailString());
      }
    }
  }

  static void addDstReference(INodeReference.DstReference d) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      validation.dstReferences.add(d);
      if (LOG.isTraceEnabled()) {
        LOG.trace("addDstReference: {}", d.toDetailString());
      }
    }
  }

  static class Ref<REF extends INodeReference> {
    private final Class<REF> clazz;
    private final List<REF> references = new LinkedList<>();
    private volatile List<Task<REF>> tasks;
    private volatile List<Future<Integer>> futures;
    private final AtomicInteger taskCompleted = new AtomicInteger();

    Ref(Class<REF> clazz) {
      this.clazz = clazz;
    }

    void add(REF ref) {
      references.add(ref);
    }

    void submit(AtomicInteger errorCount, ExecutorService service)
        throws InterruptedException {
      final int size = references.size();
      tasks = createTasks(references, errorCount);
      println("Submitting %d tasks for validating %d %s(s)",
          tasks.size(), size, clazz.getSimpleName());
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

  private final Ref<INodeReference.WithCount> withCounts
      = new Ref<>(INodeReference.WithCount.class);
  private final Ref<INodeReference.WithName> withNames
      = new Ref<>(INodeReference.WithName.class);
  private final Ref<INodeReference.DstReference> dstReferences
      = new Ref<>(INodeReference.DstReference.class);

  private int assertReferences() {
    final int p = Runtime.getRuntime().availableProcessors();
    LOG.info("Available Processors: {}", p);
    final ExecutorService service = Executors.newFixedThreadPool(p);

    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        println("ASSERT_REFERENCES Progress: %s, %s, %s",
            dstReferences, withCounts, withNames);
      }
    };
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 10_000);

    final AtomicInteger errorCount = new AtomicInteger();
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
    return errorCount.get();
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
    static final int BATCH_SIZE = 1000;

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
          println("%d: %s", errorCount.incrementAndGet(), t);
        }
      }
      return references.size();
    }
  }
}