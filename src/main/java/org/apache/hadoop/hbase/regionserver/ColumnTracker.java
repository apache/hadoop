/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

/**
 * Implementing classes of this interface will be used for the tracking
 * and enforcement of columns and numbers of versions during the course of a
 * Get or Scan operation.
 * <p>
 * Currently there are two different types of Store/Family-level queries.
 * <ul><li>{@link ExplicitColumnTracker} is used when the query specifies
 * one or more column qualifiers to return in the family.
 * <p>
 * This class is utilized by {@link ScanQueryMatcher} through two methods:
 * <ul><li>{@link #checkColumn} is called when a Put satisfies all other
 * conditions of the query.  This method returns a {@link org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode} to define
 * what action should be taken.
 * <li>{@link #update} is called at the end of every StoreFile or memstore.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
public interface ColumnTracker {
  /**
   * Keeps track of the number of versions for the columns asked for
   * @param bytes
   * @param offset
   * @param length
   * @return The match code instance.
   */
  public ScanQueryMatcher.MatchCode checkColumn(byte [] bytes, int offset, int length);

  /**
   * Updates internal variables in between files
   */
  public void update();

  /**
   * Resets the Matcher
   */
  public void reset();

  /**
   *
   * @return <code>true</code> when done.
   */
  public boolean done();

  /**
   * Used by matcher and scan/get to get a hint of the next column
   * to seek to after checkColumn() returns SKIP.  Returns the next interesting
   * column we want, or NULL there is none (wildcard scanner).
   *
   * Implementations aren't required to return anything useful unless the most recent
   * call was to checkColumn() and the return code was SKIP.  This is pretty implementation
   * detail-y, but optimizations are like that.
   *
   * @return null, or a ColumnCount that we should seek to
   */
  public ColumnCount getColumnHint();
}
