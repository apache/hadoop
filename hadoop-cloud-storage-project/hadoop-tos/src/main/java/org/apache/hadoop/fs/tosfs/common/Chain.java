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

package org.apache.hadoop.fs.tosfs.common;

import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.Queues;
import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.function.Predicate;

public final class Chain<T extends Closeable> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Chain.class);

  private final List<IOException> suppressed = Lists.newArrayList();
  private final Queue<ItemFactory<T>> queue;
  private final Predicate<IOException> shouldContinue;
  private T curItem;

  private Chain(Deque<ItemFactory<T>> queue, Predicate<IOException> shouldContinue) {
    this.queue = queue;
    this.shouldContinue = shouldContinue;
    this.curItem = null;
  }

  public <R> R run(Task<T, R> task) throws IOException {
    while (true) {
      if (curItem == null && !nextItem()) {
        IOException ex = new IOException("Failed to run task after attempt all items");
        suppressed.forEach(ex::addSuppressed);
        throw ex;
      }

      try {
        return task.run(curItem);
      } catch (IOException e) {
        LOG.debug("Encounter exception while running task with item {}", curItem, e);
        // Resetting the current caller to be null, for triggering the next round election.
        if (curItem != null) {
          CommonUtils.runQuietly(curItem::close);
          curItem = null;
        }
        suppressed.add(e);

        if (shouldContinue != null && !shouldContinue.test(e)) {
          IOException ex =
              new IOException("Failed to run the chain since the encountered error not retryable.");
          suppressed.forEach(ex::addSuppressed);
          throw ex;
        }
      }
    }
  }

  public T curItem() {
    return curItem;
  }

  private boolean nextItem() {
    if (curItem != null) {
      CommonUtils.runQuietly(curItem::close);
      curItem = null;
    }

    while (!queue.isEmpty()) {
      ItemFactory<T> nextFactory = queue.poll();
      try {
        curItem = nextFactory.newItem();
        return true;
      } catch (IOException e) {
        curItem = null;
        LOG.debug("Failed to create new item", e);
        suppressed.add(e);
      }
    }

    return false;
  }

  @Override
  public void close() throws IOException {
    if (curItem != null) {
      curItem.close();
    }
  }

  public interface ItemFactory<T extends Closeable> {
    T newItem() throws IOException;
  }

  public interface Task<T extends Closeable, R> {
    R run(T call) throws IOException;
  }

  public static class Builder<T extends Closeable> {
    private final Deque<ItemFactory<T>> factories = Queues.newArrayDeque();
    private Predicate<IOException> shouldContinue;

    public Builder<T> addFirst(ItemFactory<T> factory) {
      factories.addFirst(factory);
      return this;
    }

    public Builder<T> addLast(ItemFactory<T> factory) {
      factories.addLast(factory);
      return this;
    }

    public Builder<T> shouldContinue(Predicate<IOException> continueCondition) {
      this.shouldContinue = continueCondition;
      return this;
    }

    public Chain<T> build() throws IOException {
      Chain<T> chain = new Chain<>(factories, shouldContinue);

      // Do nothing in the chain task to initialize the first item.
      chain.run(item -> null);
      return chain;
    }
  }

  public static <T extends Closeable> Builder<T> builder() {
    return new Builder<>();
  }
}
