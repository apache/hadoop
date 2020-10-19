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

package org.apache.hadoop.fs;

import java.util.EnumSet;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/** Statistic which are collected in {@link FileSystem}. */
public enum Statistic {

  BYTES_READ("bytesRead", "Count of bytes read in read operations"),
  BYTES_WRITTEN("bytesWritten", "Count of bytes written in write operations"),
  READ_OPS("readOps", "Number of read operations"),
  LARGE_READ_OPS("largeReadOps", "Number of large read operations"),
  WRITE_OPS("writeOps", "Number of write operations"),
  BYTES_READ_LOCAL_HOST(
      "bytesReadLocalHost", "Count of bytes read from 0 network distance"),
  BYTES_READ_DISTANCE_OF_ONE_OR_TWO(
      "bytesReadDistanceOfOneOrTwo",
      "Count of bytes read from {1, 2} network distance"),
  BYTES_READ_DISTANCE_OF_THREE_OR_FOUR(
      "bytesReadDistanceOfThreeOrFour",
      "Count of bytes read from {3, 4} network distance"),
  BYTES_READ_DISTANCE_OF_FIVE_OR_LARGER(
      "bytesReadDistanceOfFiveOrLarger",
      "Count of bytes read from {5 and beyond} network distance"),
  BYTES_READ_ERASURE_CODED(
      "bytesReadErasureCoded", "Bytes read on erasure-coded files"),
  REMOTE_READ_TIME_MS(
      "remoteReadTimeMS", "Time taken to read bytes from remote");

  public static final ImmutableSet<Statistic> VALUES =
      ImmutableSet.copyOf(EnumSet.allOf(Statistic.class));

  private static final ImmutableMap<String, Statistic> SYMBOL_MAP =
      Maps.uniqueIndex(Iterators.forArray(values()), Statistic::getSymbol);

  Statistic(String symbol, String description) {
    this.symbol = symbol;
    this.description = description;
  }

  private final String symbol;
  private final String description;

  public String getSymbol() {
    return symbol;
  }

  /**
   * Get a statistic from a symbol.
   *
   * @param symbol statistic to look up
   * @return the value or null.
   */
  public static Statistic fromSymbol(String symbol) {
    return SYMBOL_MAP.get(symbol);
  }

  public String getDescription() {
    return description;
  }

  /**
   * The string value is simply the symbol. This makes this operation very low
   * cost.
   *
   * @return the symbol of this statistic.
   */
  @Override
  public String toString() {
    return symbol;
  }
}
