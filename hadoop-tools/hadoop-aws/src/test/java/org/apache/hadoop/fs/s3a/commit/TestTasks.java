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

package org.apache.hadoop.fs.s3a.commit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Tasks class.
 */
@RunWith(Parameterized.class)
public class TestTasks extends HadoopTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestTasks.class);
  public static final int ITEM_COUNT = 4;
  private static final int FAILPOINT = 2;

  private final int numThreads;
  /**
   * Thread pool for task execution.
   */
  private ExecutorService threadPool;
  private final CounterTask failingTask
      = new CounterTask("runner",FAILPOINT, Item::commit);

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
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {0},
        {1},
        {3},
    });
  }


  private List<Item> items;


  /**
   * Construct the parameterized test.
   * @param numThreads number of threads
   */
  public TestTasks(int numThreads) {
    this.numThreads = numThreads;
  }

  /**
   * In a parallel test run there is more than one thread doing the execution.
   * @return true if the threadpool size is >1
   */
  public boolean isParallel() {
    return numThreads > 1;
  }

  @Before
  public void setup() {
    items = IntStream.rangeClosed(1, ITEM_COUNT)
        .mapToObj((i) -> new Item(i))
        .collect(Collectors.toList());

    if (numThreads > 0) {
      threadPool = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat(getMethodName() + "-pool-%d")
              .build());
    }

  }

  @After
  public void teardown() {
    if (threadPool != null) {
      threadPool.shutdown();
      threadPool = null;
    }
  }

  /**
   * create the builder.
   * @return pre-inited builder
   */
  private Tasks.Builder<Item> builder() {
    return Tasks.foreach(items).executeWith(threadPool);
  }

  private void assertRun(Tasks.Builder<Item> builder,
      CounterTask task) throws IOException {
    boolean b = builder.run(task);
    assertTrue("Run of " + task + " failed", b);
  }
  
  private void assertFailed(Tasks.Builder<Item> builder,
      CounterTask task) throws IOException {
    boolean b = builder.run(task);
    assertFalse("Run of " + task + " unexpectedly succeeded", b);
  }
  
  @Test
  public void testSimpleInvocation() throws Throwable {
    CounterTask t = new CounterTask("simple", 0, Item::commit);
    assertRun(builder(), t);
    t.assertInvoked("", ITEM_COUNT);
  }

  @Test
  public void testFailOnFourNoStoppingSuppressed() throws Throwable {
    assertFailed(builder().suppressExceptions(), failingTask);
    failingTask.assertInvoked("continued through operations", ITEM_COUNT);
    items.forEach(Item::assertCommitted);
  }

  @Test
  public void testFailFastSuppressed() throws Throwable {
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

  @Test
  public void testFailedCallAbortSuppressed() throws Throwable {
    assertFailed(builder()
            .stopOnFailure()
            .suppressExceptions()
            .abortWith(aborter),
        failingTask);
    failingTask.assertInvokedAtLeast("success", FAILPOINT);
    if (!isParallel()) {
      aborter.assertInvokedAtLeast("abort", 1);
      // all uncommitted items were aborted
      items.stream().filter((i) -> !i.committed)
          .map(Item::assertAborted);
      items.stream().filter((i) -> i.committed)
          .forEach((i) -> assertFalse(i.toString(), i.aborted));
    }
  }

  @Test
  public void testFailedCalledWhenNotStoppingSuppressed() throws Throwable {
    assertFailed(builder()
            .suppressExceptions()
            .onFailure(failures),
        failingTask);
    failingTask.assertInvokedAtLeast("success", FAILPOINT);
    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }

  @Test
  public void testFailFastCallRevertSuppressed() throws Throwable {
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
      items.stream().filter((i) -> !i.committed)
          .forEach(Item::assertAborted);
    }
    // all committed were reverted
    items.stream().filter((i) -> i.committed && i.id != FAILPOINT)
        .forEach(Item::assertReverted);
    // all reverted items are committed
    items.stream().filter((i) -> i.reverted)
        .forEach(Item::assertCommitted);

    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }

  @Test
  public void testFailSlowCallRevertSuppressed() throws Throwable {
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
        .filter((i) -> i.id != failing)
        .filter((i) -> i.committed)
        .forEach(Item::assertReverted);
    // all reverted items are committed
    items.stream().filter((i) -> i.reverted)
        .forEach(Item::assertCommitted);

    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }

  @Test
  public void testFailFastExceptions() throws Throwable {
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

  @Test
  public void testFailSlowExceptions() throws Throwable {
    intercept(IOException.class,
        () -> builder()
            .run(failingTask));
    failingTask.assertInvoked("continued through operations", ITEM_COUNT);
    items.forEach(Item::assertCommitted);
  }

  @Test
  public void testFailFastExceptionsWithAbortFailure() throws Throwable {
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

  @Test
  public void testFailFastExceptionsWithAbortFailureStopped() throws Throwable {
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
  @Test
  public void testRevertAllSuppressed() throws Throwable {
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
        .filter((i) -> i.id != failing)
        .filter((i) -> i.committed)
        .forEach(Item::assertReverted);
    items.stream()
        .filter((i) -> i.id != failing)
        .filter((i) -> !i.committed)
        .forEach(Item::assertAborted);
    // all reverted items are committed
    items.stream().filter((i) -> i.reverted)
        .forEach(Item::assertCommitted);

    // only one failure was triggered
    failures.assertInvoked("failure event", 1);
  }


  /**
   * The Item which tasks process.
   */
  private static class Item {
    final int id;
    boolean committed;
    boolean aborted;
    boolean reverted;

    private Item(int item) {
      this.id = item;
    }

    boolean commit() {
      return committed = true;
    }

    boolean abort() {
      return aborted = true;
    }

    boolean revert() {
      return reverted = true;
    }

    public Item assertCommitted() {
      assertTrue(toString() + " was not committed", committed);
      return this;
    }

    public Item assertAborted() {
      assertTrue(toString() + " was not aborted", aborted);
      return this;
    }

    public Item assertReverted() {
      assertTrue(toString() + " was not reverted", reverted);
      return this;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Operation{");
      sb.append("id=").append(id);
      sb.append(", committed=").append(committed);
      sb.append(", aborted=").append(aborted);
      sb.append(", reverted=").append(reverted);
      sb.append('}');
      return sb.toString();
    }
  }


  /**
   * Class which can count invocations and, if limit > 0, will raise
   * an exception on the specific invocation of {@link #note(Object)}
   * whose count == limit.
   */
  private static class BaseCounter {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int limit;
    private final String name;
    private Item item;
    private final Optional<Function<Item, Boolean>> action;

    /**
     * Base counter, tracks items
     * @param name name for string/exception/logs.
     * @param limit limit at which an exception is raised, 0 == never
     * @param action optional action to invoke after the increment,
     * before limit check
     */
    public BaseCounter(String name,
        int limit,
        Function<Item, Boolean> action) {
      this.name = name;
      this.limit = limit;
      this.action = Optional.ofNullable(action);
    }

    void process(Item item) throws IOException {
      this.item = item;
      action.map(a -> a.apply(item));
      int count = counter.incrementAndGet();
      LOG.info("{}: processed({})", this, item);
      if (limit == count) {
        throw new IOException(String.format("%s: Limit %d reached",
            this, limit, item));
      }
    }

    int getCount() {
      return counter.get();
    }

    Item getItem() {
      return item;
    }

    public void assertInvoked(String text, int expected) {
      assertEquals(toString() + ": " + text, expected, getCount());
    }

    public void assertInvokedAtLeast(String text, int expected) {
      int actual = getCount();
      assertTrue(toString() + ": " + text
              + "-expected " + expected
              + " invocations, but got " + actual,
          expected <= actual);
    }

    public void assertNotInvoked(String text) {
      assertInvoked(text, 0);
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

  private static class CounterTask
      extends BaseCounter implements Tasks.Task<Item, IOException> {

    public CounterTask(String name) {
      this(name, 0, null);
    }

    public CounterTask(String name, int limit,
        Function<Item, Boolean> action) {
      super(name, limit, action);
    }

    @Override
    public void run(Item item) throws IOException {
      process(item);
    }

  }


  private static class FailureCounter
      extends BaseCounter implements Tasks.FailureTask<Item, IOException> {
    private Exception exception;

    public FailureCounter(String name, int limit,
        Function<Item, Boolean> action) {
      super(name, limit, action);
    }

    @Override
    public void run(Item item, Exception exception) throws IOException {
      process(item);
      this.exception = exception;
    }

    Exception getException() {
      return exception;
    }


  }

}


