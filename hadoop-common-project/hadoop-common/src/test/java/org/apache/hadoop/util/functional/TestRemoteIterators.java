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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.functional.RemoteIterators.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link RemoteIterators}.
 *
 */
public class TestRemoteIterators extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestRemoteIterators.class);

  private static final String[] DATA = {"a", "b", "c"};

  /** Counter for lambda-expressions. */
  private int counter;

  @Test
  public void testIterateArray() throws Throwable {
    verifyInvoked(remoteIteratorFromArray(DATA), DATA.length,
        (s) -> LOG.info(s));
  }

  @Test
  public void testIterateArrayMapped() throws Throwable {
    verifyInvoked(
        mappingRemoteIterator(
            remoteIteratorFromArray(DATA),
            (d) -> {
              counter += d.length();
              return d;
            }),
        DATA.length,
        this::log);
    assertCounterValue(3);
  }

  public void log(Object o) {
    LOG.info("{}", o);
  }

  /**
   * Singleton is iterated through once.
   * The toString() call is passed through.
   */
  @Test
  public void testSingleton() throws Throwable {
    StringBuffer result = new StringBuffer();
    String name = "singleton";
    RemoteIterator<String> it = remoteIteratorFromSingleton(name);
    assertStringValueContains(it, "SingletonIterator");
    assertStringValueContains(it, name);
    verifyInvoked(
        it,
        1,
        (s) -> result.append(s));
    assertThat(result.toString())
        .isEqualTo(name);
  }

  @Test
  public void testSingletonNotClosed() throws Throwable {
    CloseCounter closeCounter = new CloseCounter();
    RemoteIterator<CloseCounter> it = remoteIteratorFromSingleton(closeCounter);
    verifyInvoked(it, 1, this::log);
    close(it);
    closeCounter.assertCloseCount(0);
  }

  /**
   * A null singleton is not an error.
   */
  @Test
  public void testNullSingleton() throws Throwable {
    verifyInvoked(remoteIteratorFromSingleton(null), 0, this::log);
  }


  /**
   * If you create a singleton iterator and it is an IOStatisticsSource,
   * then that is the statistics which can be extracted from the
   * iterator.
   */
  @Test
  public void testSingletonStats() throws Throwable {
    IOStatsInstance singleton = new IOStatsInstance();
    RemoteIterator<IOStatsInstance> it
        = remoteIteratorFromSingleton(singleton);
    extractStatistics(it);
  }

  /**
   * The mapping remote iterator passes IOStatistics
   * calls down.
   */
  @Test
  public void testMappedSingletonStats() throws Throwable {
    IOStatsInstance singleton = new IOStatsInstance();
    RemoteIterator<String> it
        = mappingRemoteIterator(remoteIteratorFromSingleton(singleton),
        Object::toString);
    verifyInvoked(it, 1, this::log);
    extractStatistics(it);
  }

  /**
   * Close() calls are passed through.
   */
  @Test
  public void testClosePassthrough() throws Throwable {
    CountdownRemoteIterator countdown = new CountdownRemoteIterator(0);
    RemoteIterator<Integer> it = mappingRemoteIterator(
        countdown,
        i -> i);
    verifyInvoked(it, 0, this::log);
    // the foreach() operation called close()
    countdown.assertCloseCount(1);
    extractStatistics(countdown);
    ((Closeable)it).close();
    countdown.assertCloseCount(1);
  }

  @Test
  public void testMapping() throws Throwable {
    CountdownRemoteIterator countdown = new CountdownRemoteIterator(100);
    RemoteIterator<Integer> it = mappingRemoteIterator(
        countdown,
        i -> i);
    verifyInvoked(it, 100, c -> counter++);
    assertCounterValue(100);
    extractStatistics(it);
    assertStringValueContains(it, "CountdownRemoteIterator");
    close(it);
    countdown.assertCloseCount(1);
  }

  @Test
  public void testFiltering() throws Throwable {
    CountdownRemoteIterator countdown = new CountdownRemoteIterator(100);
    // only even numbers are passed through
    RemoteIterator<Integer> it = filteringRemoteIterator(
        countdown,
        i -> (i % 2) == 0);
    verifyInvoked(it, 50, c -> counter++);
    assertCounterValue(50);
    extractStatistics(it);
    close(it);
    countdown.assertCloseCount(1);
  }

  /**
   * A filter which accepts nothing results in
   * an empty iteration.
   */
  @Test
  public void testFilterNoneAccepted() throws Throwable {
    // nothing gets through
    RemoteIterator<Integer> it = filteringRemoteIterator(
        new CountdownRemoteIterator(100),
        i -> false);
    verifyInvoked(it, 0, c -> counter++);
    assertCounterValue(0);
    extractStatistics(it);
  }

  @Test
  public void testFilterAllAccepted() throws Throwable {
    // nothing gets through
    RemoteIterator<Integer> it = filteringRemoteIterator(
        new CountdownRemoteIterator(100),
        i -> true);
    verifyInvoked(it, 100, c -> counter++);
    assertStringValueContains(it, "CountdownRemoteIterator");
  }

  @Test
  public void testJavaIteratorSupport() throws Throwable {
    CountdownIterator countdownIterator = new CountdownIterator(100);
    RemoteIterator<Integer> it = remoteIteratorFromIterator(
        countdownIterator);
    verifyInvoked(it, 100, c -> counter++);
    assertStringValueContains(it, "CountdownIterator");
    extractStatistics(it);
    close(it);
    countdownIterator.assertCloseCount(1);
  }

  @Test
  public void testJavaIterableSupport() throws Throwable {
    CountdownIterable countdown = new CountdownIterable(100);
    RemoteIterator<Integer> it = remoteIteratorFromIterable(
        countdown);
    verifyInvoked(it, 100, c -> counter++);
    assertStringValueContains(it, "CountdownIterator");
    extractStatistics(it);
    // close the iterator
    close(it);
    countdown.assertCloseCount(0);
    // and a new iterator can be crated
    verifyInvoked(remoteIteratorFromIterable(countdown),
        100, c -> counter++);
  }

  /**
   * If a RemoteIterator is constructed from an iterable
   * and that is to be closed, we close it.
   */
  @Test
  public void testJavaIterableClose() throws Throwable {
    CountdownIterable countdown = new CountdownIterable(100);
    RemoteIterator<Integer> it = closingRemoteIterator(
        remoteIteratorFromIterable(countdown),
        countdown);
    verifyInvoked(it, 100, c -> counter++);
    assertStringValueContains(it, "CountdownIterator");
    extractStatistics(it);

    // verify the iterator was self closed in hasNext()
    countdown.assertCloseCount(1);

    // explicitly close the iterator
    close(it);
    countdown.assertCloseCount(1);
    // and a new iterator cannot be created
    intercept(IllegalStateException.class, () ->
        remoteIteratorFromIterable(countdown));
  }

  /**
   * If a RemoteIterator is constructed from an iterable
   * and that is to be closed, we close it.
   */
  @SuppressWarnings("InfiniteLoopStatement")
  @Test
  public void testJavaIterableCloseInNextLoop() throws Throwable {
    CountdownIterable countdown = new CountdownIterable(100);
    RemoteIterator<Integer> it = closingRemoteIterator(
        remoteIteratorFromIterable(countdown),
        countdown);
    try {
      while(true) {
        it.next();
      }
    } catch (NoSuchElementException expected) {

    }
    // verify the iterator was self closed in next()
    countdown.assertCloseCount(1);

  }

  /**
   * assert that the string value of an object contains the
   * expected text.
   * @param o object
   * @param expected  expected text
   */
  protected void assertStringValueContains(
      final Object o,
      final String expected) {
    assertThat(o.toString())
        .describedAs("Object string value")
        .contains(expected);
  }

  /**
   * Assert that the counter field is at a specific value.
   * @param expected counter
   */
  protected void assertCounterValue(final int expected) {
    assertThat(counter)
        .describedAs("Counter value")
        .isEqualTo(expected);
  }

  /**
   * Verify that the iteration completes with a given size.
   * @param it iterator
   * @param <T> type.
   * @param length expected size
   * @param consumer consumer
   */
  protected <T> void verifyInvoked(final RemoteIterator<T> it,
      int length,
      ConsumerRaisingIOE<T> consumer)
      throws IOException {
    assertThat(foreach(it, consumer))
        .describedAs("Scan through iterator %s", it)
        .isEqualTo(length);
  }

  /**
   * Close an iterator if it is iterable.
   * @param it iterator
   * @param <T> type.
   */
  private <T> void close(final RemoteIterator<T> it) throws IOException {
    if (it instanceof Closeable) {
      ((Closeable) it).close();
    }
  }

  /**
   * Class whose close() call increments a counter.
   */
  private static class CloseCounter extends
      IOStatsInstance implements Closeable {

    private int closeCount;

    @Override
    public void close() throws IOException {
      closeCount++;
      LOG.info("close ${}", closeCount);
    }

    public int getCloseCount() {
      return closeCount;
    }

    public void reset() {
      closeCount = 0;
    }

    public void assertCloseCount(int expected) {
      assertThat(closeCount)
          .describedAs("Close count")
          .isEqualTo(expected);
    }

  }

  /**
   * Simple class to implement IOStatistics.
   */
  private static class IOStatsInstance implements IOStatisticsSource {

    private IOStatisticsSnapshot stats = new IOStatisticsSnapshot();

    @Override
    public IOStatistics getIOStatistics() {
      return stats;
    }

  }

  /**
   * Iterator which counts down.
   */
  private static final class CountdownRemoteIterator extends CloseCounter
      implements RemoteIterator<Integer> {

    private int limit;

    private CountdownRemoteIterator(final int limit) {
      this.limit = limit;
    }

    @Override
    public boolean hasNext() throws IOException {
      return limit > 0;
    }

    @Override
    public Integer next() throws IOException {
      return limit--;
    }

    @Override
    public String toString() {
      return "CountdownRemoteIterator{" +
          "limit=" + limit +
          '}';
    }
  }

  /**
   * Iterator which counts down.
   */
  private static final class CountdownIterator extends CloseCounter
      implements Iterator<Integer> {

    private int limit;

    private CountdownIterator(final int limit) {
      this.limit = limit;
    }

    @Override
    public boolean hasNext() {
      return limit > 0;
    }

    @Override
    public Integer next() {
      if (!hasNext()) {
        throw new NoSuchElementException("limit reached");
      }
      return limit--;
    }

    @Override
    public String toString() {
      return "CountdownIterator{" +
          "limit=" + limit +
          '}';
    }
  }

  /**
   * Iterable for countdown iterators.
   * Once closed, calls to iterator() raise an exception.
   */
  private static final class CountdownIterable extends CloseCounter
      implements Iterable<Integer> {

    private int limit;

    private CountdownIterable(final int limit) {
      this.limit = limit;
    }

    @Override
    public Iterator<Integer> iterator() {
      Preconditions.checkState(getCloseCount() == 0);

      return new CountdownIterator(limit);
    }
  }

}
