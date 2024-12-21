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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * PreferredIterator implements an Iterator that prioritizes iterating over a subset of items
 * based on a preference ratio. It allows specifying a ratio of preferred items and
 * a ratio of ignored items. The remaining items are iterated over after all preferred items
 * have been visited.
 */
public class PreferredIterator<N> implements Iterator<N> {

  private static final Logger LOG = LoggerFactory.getLogger(PreferredIterator.class);
  final private List<N> preferred, others;
  final private int totalNum, visibleNum;
  private int visitedNum, preferredIndex, othersIndex;

  public PreferredIterator(float preferRatio, float ignoreRatio, List<N> items) {
    if (preferRatio < 0 || preferRatio > 1) {
      LOG.warn("preferRatio must be in [0, 1], but got {}, setting to 0.",
          preferRatio);
      preferRatio = 0;
    }
    if (ignoreRatio < 0 || ignoreRatio > 1) {
      LOG.warn("ignoreRatio must be in [0, 1], but got {}, setting to 0.",
          ignoreRatio);
      ignoreRatio = 0;
    }
    if (preferRatio + ignoreRatio > 1) {
      LOG.warn("preferRatio + dropRatio must be <= 1, but got {}, "
          + "setting dropRatio to 0.", preferRatio+ignoreRatio);
      ignoreRatio = 0;
    }
    totalNum = items.size();
    int splitIndex = (int) Math.ceil(preferRatio * totalNum);
    preferred = items.subList(0, splitIndex);
    int othersEndIndex = totalNum;
    if (ignoreRatio > 0) {
      int dropNum = (int) Math.ceil(ignoreRatio * totalNum);
      othersEndIndex = totalNum - dropNum;
    }
    others = items.subList(splitIndex, othersEndIndex);
    visibleNum = preferred.size() + others.size();
    reinitialize();
  }

  @Override
  public boolean hasNext() {
    return visitedNum < visibleNum;
  }

  @Override
  public N next() {
    if (!hasNext()) {
      return null;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("next: totalNum={}, preferredNum={}, othersNum={}, "
              + "visited={}, preferredIndex={}, othersIndex={}", totalNum,
          preferred.size(), others.size(), visitedNum, preferredIndex,
          othersIndex);
    }
    // prioritize iterating over the preferred items
    if (visitedNum < preferred.size()) {
      N item = preferred.get(preferredIndex);
      preferredIndex = (preferredIndex + 1) % preferred.size();
      visitedNum++;
      return item;
    }
    // iterate over the others
    N item = others.get(othersIndex++);
    visitedNum++;
    return item;
  }

  /**
   * Reinitialize the iterator.
   * - shuffle the preferred index
   * - reset visitedNum and othersIndex to 0
   */
  public void reinitialize() {
    Random rand = new Random();
    preferredIndex = preferred.isEmpty() ? 0 : rand.nextInt(preferred.size());
    visitedNum = 0;
    othersIndex = 0;
    LOG.info("Initialized: totalNum={}, visibleNum={}, "
            + "preferredNum={}, othersNum={}, visited={}, preferredIndex={},"
            + " othersIndex={}", totalNum, visibleNum, preferred.size(),
        others.size(), visitedNum, preferredIndex, othersIndex);
  }
}
