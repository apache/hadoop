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

package org.apache.hadoop.util.functional;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Task Pool class.
 * This is pulled straight out of the S3A version.
 */
public class TestTaskPool extends HadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestTaskPool.class);

  public static final int ITEM_COUNT = 16;

  private static final int FAILPOINT = 8;

  private int numThreads;

  /**
   * Thread pool for task execution.
   */
  private ExecutorService threadPool;

  /**
   * Task submitter bonded to the thread pool, or
   * null for the 0-thread case.
   */
  private TaskPool.Submitter submitter;

  private final CounterTask failingTask
      = new CounterTask("failing committer", FAILPOINT, Item::commit);

  private final FailureCounter failures
      = new FailureCounter("failures", 0, null);

  private final CounterTask reverter
      = new CounterTask("reverter", 0, Item::revert);

  private final CounterTask aborter
      = new CounterTask("aborter", 0, Item::abort);

  /**
   * Test array for parameterized test runs: how many threads and
   * to use. Threading makes some of the assertions brittle; there are
   * more checks on single thread than parallel ops.
   * @return a list of parameter tuples.
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {0},
        {1},
        {3},
        {8},
        {16},
    });
  }

  private List<Item> items;

  /**
   * Construct the parameterized test.
   * @param pNumThreads number of threads
   */
  public void initTestTaskPool(int pNumThreads) {
    this.numThreads = pNumThreads;
  }

  /**
   * In a parallel test run there is more than one thread doing the execution.
   * @return true if the threadpool size is >1
   */
  public boolean isParallel() {
    return numThreads > 1;
  }

  @BeforeEach
  public void setup() {
    items = IntStream.rangeClosed(1, ITEM_COUNT)
        .mapToObj(i -> new Item(i,
            String.format("With %d threads", numThreads)))
        .collect(Collectors.toList());

    if (numThreads > 0) {
      threadPool = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat(getMethodName() + "-pool-%d")
              .build());
      submitter = new PoolSubmitter();
    } else {
      submitter = null;
    }

  }

  @AfterEach
  public void teardown() {
    if (threadPool != null) {
      threadPool.shutdown();
      threadPool = null;
    }
  }

  private class PoolSubmitter implements TaskPool.Submitter {

    @Override
    public Future<?> submit(final Runnable task) {
      return threadPool.submit(task);
    }

  }

  /**
   * create the builder.
   * @return pre-inited builder
   */
  private TaskPool.Builder<Item> builder() {
    return TaskPool.foreach(items).executeWith(submitter);
  }

  private void assertRun(TaskPool.Builder<Item> builder,
      CounterTask task) throws IOException {
    boolean b = builder.run(task);
    assertTrue(b, "Run of " + task + " failed");
  }

  private void assertFailed(TaskPool.Builder<Item> builder,
      CounterTask task) throws IOException {
    boolean b = builder.run(task);
    assertFalse(b, "Run of " + task + " unexpectedly succeeded");
  }

  private String itemsToString() {
    return "[" + items.stream().map(Item::toString)
        .collect(Collectors.joining("\n")) + "]";
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testSimpleInvocation(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    CounterTask t = new CounterTask("simple", 0, Item::commit);
    assertRun(builder(), t);
    t.assertInvoked("", ITEM_COUNT);
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailNoStoppingSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    assertFailed(builder().suppressExceptions(), failingTask);
    failingTask.assertInvoked("Continued through operations", ITEM_COUNT);
    items.forEach(Item::assertCommittedOrFailed);
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailFastSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    assertFailed(builder()
            .suppressExceptions()
            .stopOnFailure(),
        failingTask);
    if (isParallel()) {
      failingTask.assertInvokedAtLeast("stop fast", FAILPOINT);
    } else {
      failingTask.assertInvoked("stop fast", FAILPOINT);
    }
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailedCallAbortSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    assertFailed(builder()
            .stopOnFailure()
            .suppressExceptions()
            .abortWith(aborter),
        failingTask);
    failingTask.assertInvokedAtLeast("success", FAILPOINT);
    if (!isParallel()) {
      aborter.assertInvokedAtLeast("abort", 1);
      // all uncommitted items were aborted
      items.stream().filter(i -> !i.committed)
          .map(Item::assertAborted);
      items.stream().filter(i -> i.committed)
          .forEach(i -> assertFalse(i.aborted, i.toString()));
    }
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailedCalledWhenNotStoppingSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    assertFailed(builder()
            .suppressExceptions()
            .onFailure(failures),
        failingTask);
    failingTask.assertInvokedAtLeast("success", FAILPOINT);
    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailFastCallRevertSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    assertFailed(builder()
            .stopOnFailure()
            .revertWith(reverter)
            .abortWith(aborter)
            .suppressExceptions()
            .onFailure(failures),
        failingTask);
    failingTask.assertInvokedAtLeast("success", FAILPOINT);
    if (!isParallel()) {
      aborter.assertInvokedAtLeast("abort", 1);
      // all uncommitted items were aborted
      items.stream().filter(i -> !i.committed)
          .filter(i -> !i.failed)
          .forEach(Item::assertAborted);
    }
    // all committed were reverted
    items.stream().filter(i -> i.committed && !i.failed)
        .forEach(Item::assertReverted);
    // all reverted items are committed
    items.stream().filter(i -> i.reverted)
        .forEach(Item::assertCommitted);

    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailSlowCallRevertSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    assertFailed(builder()
            .suppressExceptions()
            .revertWith(reverter)
            .onFailure(failures),
        failingTask);
    failingTask.assertInvokedAtLeast("success", FAILPOINT);
    // all committed were reverted
    // identify which task failed from the set
    int failing = failures.getItem().id;
    items.stream()
        .filter(i -> i.id != failing)
        .filter(i -> i.committed)
        .forEach(Item::assertReverted);
    // all reverted items are committed
    items.stream().filter(i -> i.reverted)
        .forEach(Item::assertCommitted);

    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailFastExceptions(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    intercept(IOException.class,
        () -> builder()
            .stopOnFailure()
            .run(failingTask));
    if (isParallel()) {
      failingTask.assertInvokedAtLeast("stop fast", FAILPOINT);
    } else {
      failingTask.assertInvoked("stop fast", FAILPOINT);
    }
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailSlowExceptions(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    intercept(IOException.class,
        () -> builder()
            .run(failingTask));
    failingTask.assertInvoked("continued through operations", ITEM_COUNT);
    items.forEach(Item::assertCommittedOrFailed);
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailFastExceptionsWithAbortFailure(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    CounterTask failFirst = new CounterTask("task", 1, Item::commit);
    CounterTask a = new CounterTask("aborter", 1, Item::abort);
    intercept(IOException.class,
        () -> builder()
            .stopOnFailure()
            .abortWith(a)
            .run(failFirst));
    if (!isParallel()) {
      // expect the other tasks to be aborted
      a.assertInvokedAtLeast("abort", ITEM_COUNT - 1);
    }
  }

  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testFailFastExceptionsWithAbortFailureStopped(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    CounterTask failFirst = new CounterTask("task", 1, Item::commit);
    CounterTask a = new CounterTask("aborter", 1, Item::abort);
    intercept(IOException.class,
        () -> builder()
            .stopOnFailure()
            .stopAbortsOnFailure()
            .abortWith(a)
            .run(failFirst));
    if (!isParallel()) {
      // expect the other tasks to be aborted
      a.assertInvoked("abort", 1);
    }
  }

  /**
   * Fail the last one committed, all the rest will be reverted.
   * The actual ID of the last task has to be picke dup from the
   * failure callback, as in the pool it may be one of any.
   */
  @ParameterizedTest(name = "threads={0}")
  @MethodSource("params")
  public void testRevertAllSuppressed(int pNumThreads) throws Throwable {
    initTestTaskPool(pNumThreads);
    CounterTask failLast = new CounterTask("task", ITEM_COUNT, Item::commit);

    assertFailed(builder()
            .suppressExceptions()
            .stopOnFailure()
            .revertWith(reverter)
            .abortWith(aborter)
            .onFailure(failures),
        failLast);
    failLast.assertInvoked("success", ITEM_COUNT);
    int abCount = aborter.getCount();
    int revCount = reverter.getCount();
    assertEquals(ITEM_COUNT, 1 + abCount + revCount);
    // identify which task failed from the set
    int failing = failures.getItem().id;
    // all committed were reverted
    items.stream()
        .filter(i -> i.id != failing)
        .filter(i -> i.committed)
        .forEach(Item::assertReverted);
    items.stream()
        .filter(i -> i.id != failing)
        .filter(i -> !i.committed)
        .forEach(Item::assertAborted);
    // all reverted items are committed
    items.stream().filter(i -> i.reverted)
        .forEach(Item::assertCommitted);

    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }


  /**
   * The Item which tasks process.
   */
  private final class Item {

    private final int id;

    private final String text;

    private volatile boolean committed, aborted, reverted, failed;

    private Item(int item, String text) {
      this.id = item;
      this.text = text;
    }

    boolean commit() {
      committed = true;
      return true;
    }

    boolean abort() {
      aborted = true;
      return true;
    }

    boolean revert() {
      reverted = true;
      return true;
    }

    boolean fail() {
      failed = true;
      return true;
    }

    public Item assertCommitted() {
      assertTrue(committed, toString() + " was not committed in\n"
          + itemsToString());
      return this;
    }

    public Item assertCommittedOrFailed() {
      assertTrue(committed || failed,
          toString() + " was not committed nor failed in\n"
          + itemsToString());
      return this;
    }

    public Item assertAborted() {
      assertTrue(aborted, toString() + " was not aborted in\n"
          + itemsToString());
      return this;
    }

    public Item assertReverted() {
      assertTrue(reverted, toString() + " was not reverted in\n"
          + itemsToString());
      return this;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Item{");
      sb.append(String.format("[%02d]", id));
      sb.append(", committed=").append(committed);
      sb.append(", aborted=").append(aborted);
      sb.append(", reverted=").append(reverted);
      sb.append(", failed=").append(failed);
      sb.append(", text=").append(text);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Class which can count invocations and, if limit > 0, will raise
   * an exception on the specific invocation of {@link #note(Object)}
   * whose count == limit.
   */
  private class BaseCounter {

    private final AtomicInteger counter = new AtomicInteger(0);

    private final int limit;

    private final String name;

    private Item item;

    private final Optional<Function<Item, Boolean>> action;

    /**
     * Base counter, tracks items.
     * @param name name for string/exception/logs.
     * @param limit limit at which an exception is raised, 0 == never
     * @param action optional action to invoke after the increment,
     * before limit check
     */
    BaseCounter(String name,
        int limit,
        Function<Item, Boolean> action) {
      this.name = name;
      this.limit = limit;
      this.action = Optional.ofNullable(action);
    }

    /**
     * Apply the action to an item; log at info afterwards with both the
     * before and after string values of the item.
     * @param i item to process.
     * @throws IOException failure in the action
     */
    void process(Item i) throws IOException {
      this.item = i;
      int count = counter.incrementAndGet();
      if (limit == count) {
        i.fail();
        LOG.info("{}: Failed {}", this, i);
        throw new IOException(String.format("%s: Limit %d reached for %s",
            this, limit, i));
      }
      String before = i.toString();
      action.map(a -> a.apply(i));
      LOG.info("{}: {} -> {}", this, before, i);
    }

    int getCount() {
      return counter.get();
    }

    Item getItem() {
      return item;
    }

    void assertInvoked(String text, int expected) {
      assertEquals(expected, getCount(), toString() + ": " + text);
    }

    void assertInvokedAtLeast(String text, int expected) {
      int actual = getCount();
      assertTrue(expected <= actual, toString() + ": " + text
          + "-expected " + expected
          + " invocations, but got " + actual
          + " in " + itemsToString());
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "BaseCounter{");
      sb.append("name='").append(name).append('\'');
      sb.append(", count=").append(counter.get());
      sb.append(", limit=").append(limit);
      sb.append(", item=").append(item);
      sb.append('}');
      return sb.toString();
    }
  }

  private final class CounterTask
      extends BaseCounter implements TaskPool.Task<Item, IOException> {

    private CounterTask(String name, int limit,
        Function<Item, Boolean> action) {
      super(name, limit, action);
    }

    @Override
    public void run(Item item) throws IOException {
      process(item);
    }

  }

  private final class FailureCounter
      extends BaseCounter
      implements TaskPool.FailureTask<Item, IOException> {

    private Exception exception;

    private FailureCounter(String name, int limit,
        Function<Item, Boolean> action) {
      super(name, limit, action);
    }

    @Override
    public void run(Item item, Exception ex) throws IOException {
      process(item);
      this.exception = ex;
    }

    private Exception getException() {
      return exception;
    }

  }

}
