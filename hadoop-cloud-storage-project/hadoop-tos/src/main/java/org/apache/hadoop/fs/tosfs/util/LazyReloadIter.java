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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.hadoop.util.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class LazyReloadIter<T> implements Iterator<T> {
  private final List<T> buf = Lists.newArrayList();
  private final Reload<T> reload;
  private int cur = 0;
  private boolean exhausted = false;

  public LazyReloadIter(Reload<T> reload) {
    this.reload = reload;
  }

  @Override
  public boolean hasNext() {
    if (exhausted && buf.isEmpty()) {
      return false;
    }
    if (cur >= buf.size()) {
      // Reset the buffer and load more elements.
      buf.clear();
      cur = 0;
      // Reload the next batch.
      boolean exhaust = reload.fill(buf);
      while (buf.isEmpty() && !exhaust) {
        exhaust = reload.fill(buf);
      }

      if (exhaust) {
        this.exhausted = true;
      }

      return !buf.isEmpty();
    }
    return true;
  }

  @Override
  public T next() {
    if (hasNext()) {
      return buf.get(cur++);
    } else {
      throw new NoSuchElementException();
    }
  }
}
