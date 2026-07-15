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

package org.apache.hadoop.fs.azurebfs.enums;

/**
 * Defines the strategy used for vectored reads in ABFS.
 *
 * <p>
 * The strategy controls how read ranges are planned and executed, trading off
 * between request parallelism and per-request payload size.
 * </p>
 */
public enum VectoredReadStrategy {

  /**
   * Optimizes for transactions per second (TPS).
   */
  TPS_OPTIMIZED("TPS"),

  /**
   * Optimizes for overall data throughput.
   */
  THROUGHPUT_OPTIMIZED("THROUGHPUT");

  /** Short name used for configuration and logging. */
  private final String name;

  /**
   * Constructs a vectored read strategy with a short, user-friendly name.
   *
   * @param name short identifier for the strategy
   */
  VectoredReadStrategy(String name) {
    this.name = name;
  }

  /**
   * Returns the short name of the vectored read strategy.
   *
   * @return short strategy name
   */
  public String getName() {
    return name;
  }

  /**
   * Parses a configuration value into a {@link VectoredReadStrategy}.
   * @param value configuration value
   * @return matching vectored read strategy
   * @throws IllegalArgumentException if the value is invalid
   */
  public static VectoredReadStrategy fromString(String value) {
    for (VectoredReadStrategy strategy : values()) {
      if (strategy.name().equalsIgnoreCase(value)
          || strategy.getName().equalsIgnoreCase(value)) {
        return strategy;
      }
    }
    throw new IllegalArgumentException("Invalid vectored read strategy: " + value);
  }
}
