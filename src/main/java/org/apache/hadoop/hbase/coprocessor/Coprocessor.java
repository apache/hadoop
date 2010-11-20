/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Coprocess interface.
 */
public interface Coprocessor {
  public static final int VERSION = 1;
  /**
   * Installation priority. Coprocessors will be executed in sequence
   * by the order of coprocessor priority.
   */
  public enum Priority {
    HIGHEST(0),
    SYSTEM(Integer.MAX_VALUE/4),
    USER(Integer.MAX_VALUE/2),
    LOWEST(Integer.MAX_VALUE);

    private int prio;

    Priority(int prio) {
      this.prio = prio;
    }

    public int intValue() {
      return prio;
    }
  }

  // Interface
  /**
   * Called before the region is reported as open to the master.
   * @param e the environment provided by the region server
   */
  public void preOpen(final CoprocessorEnvironment e);

  /**
   * Called after the region is reported as open to the master.
   * @param e the environment provided by the region server
   */
  public void postOpen(final CoprocessorEnvironment e);

  /**
   * Called before the memstore is flushed to disk.
   * @param e the environment provided by the region server
   */
  public void preFlush(final CoprocessorEnvironment e);

  /**
   * Called after the memstore is flushed to disk.
   * @param e the environment provided by the region server
   */
  public void postFlush(final CoprocessorEnvironment e);

  /**
   * Called before compaction.
   * @param e the environment provided by the region server
   * @param willSplit true if compaction will result in a split, false
   * otherwise
   */
  public void preCompact(final CoprocessorEnvironment e,
    final boolean willSplit);

  /**
   * Called after compaction.
   * @param e the environment provided by the region server
   * @param willSplit true if compaction will result in a split, false
   * otherwise
   */
  public void postCompact(final CoprocessorEnvironment e,
    final boolean willSplit);

  /**
   * Called before the region is split.
   * @param e the environment provided by the region server
   * (e.getRegion() returns the parent region)
   */
  public void preSplit(final CoprocessorEnvironment e);

  /**
   * Called after the region is split.
   * @param e the environment provided by the region server
   * (e.getRegion() returns the parent region)
   * @param l the left daughter region
   * @param r the right daughter region
   */
  public void postSplit(final CoprocessorEnvironment e, final HRegion l,
    final HRegion r);

  /**
   * Called before the region is reported as closed to the master.
   * @param e the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  public void preClose(final CoprocessorEnvironment e, boolean abortRequested);

  /**
   * Called after the region is reported as closed to the master.
   * @param e the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  public void postClose(final CoprocessorEnvironment e, boolean abortRequested);
}
