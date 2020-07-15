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

import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** For validating {@link INodeReference} subclasses. */
public class INodeReferenceValidation {
  public static final Logger LOG = LoggerFactory.getLogger(INodeReference.class);

  private static final AtomicReference<INodeReferenceValidation> INSTANCE = new AtomicReference<>();

  public static void start() {
    INSTANCE.compareAndSet(null, new INodeReferenceValidation());
    LOG.info("Validation started");
  }

  public static int end() {
    final INodeReferenceValidation instance = INSTANCE.getAndSet(null);
    if (instance == null) {
      return 0;
    }

    final int errorCount = instance.assertReferences();
    LOG.info("Validation ended successfully: {} error(s) found.", errorCount);
    return errorCount;
  }

  static void addWithCount(INodeReference.WithCount c) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      validation.withCounts.add(c);
      LOG.info("addWithCount: " + c.toDetailString());
    }
  }

  static void addWithName(INodeReference.WithName n) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      validation.withNames.add(n);
      LOG.info("addWithName: {}", n.toDetailString());
    }
  }

  static void addDstReference(INodeReference.DstReference d) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      validation.dstReferences.add(d);
      LOG.info("addDstReference: {}", d.toDetailString());
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
      LOG.info("Submitting {} tasks for validating {} {}(s)",
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
    final int availableProcessors = Runtime.getRuntime().availableProcessors();
    LOG.info("Available Processors: {}", availableProcessors);
    final ExecutorService service = Executors.newFixedThreadPool(availableProcessors);

    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        LOG.info("ASSERT_REFERENCES Progress: {}, {}, {}",
            dstReferences, withCounts, withNames);
      }
    };
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 60_000);

    final AtomicInteger errorCount = new AtomicInteger();
    try {
      dstReferences.submit(errorCount, service);
      withCounts.submit(errorCount, service);
      withNames.submit(errorCount, service);

      dstReferences.waitForFutures();
      withCounts.waitForFutures();
      withNames.waitForFutures();
    } catch (Throwable e) {
      LOG.error("Failed to assertReferences", e);
    } finally {
      service.shutdown();
      t.cancel();
    }
    return errorCount.get();
  }

  static <REF extends INodeReference> List<Task<REF>> createTasks(
      List<REF> references, AtomicInteger errorCount) {
    final List<Task<REF>> tasks = new LinkedList<>();
    for (final Iterator<REF> i = references.iterator(); i.hasNext(); ) {
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
          LOG.error("{}: {}", errorCount.incrementAndGet(), t);
        }
      }
      return references.size();
    }
  }
}