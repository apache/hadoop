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
package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

/**
 * Base class for numeric {@link Expression}s.
 * Implements helper methods for processing numeric arguments.
 */
public abstract class NumberExpression extends BaseExpression {
  /**
   * Number of milliseconds in a day.
   */
  protected static final long DAY_IN_MILLISECONDS = 86400000L;

  /**
   * Number of milliseconds in a minute.
   */
  protected static final long MINUTE_IN_MILLISECONDS = 60000L;

  private long max = Long.MIN_VALUE;
  private long min = Long.MIN_VALUE;
  private long units = 1L;

  protected enum MODE {
    ROUND_UP, ROUND_DOWN,
  }

  private MODE mode = MODE.ROUND_DOWN;

  /**
   * Constructor specifying for size of units to used as the expression
   * argument.
   *
   * @param units size of the expression argument compared to the item being
   *              processed.
   */
  protected NumberExpression(long units) {
    setUnits(units);
  }

  protected NumberExpression() {
    this(1L);
  }

  protected void setUnits(long units) {
    this.units = units;
  }

  protected void setMode(MODE mode) {
    this.mode = mode;
  }

  @Override
  public void prepare() throws IOException {
    parseArgument(getArgument(1));
  }

  /**
   * Parse the argument string to extract the numeric argument.
   *
   * @param arg String to be parsed
   * @throws IllegalArgumentException if there is a problem parsing the argument
   */
  protected void parseArgument(String arg) throws IllegalArgumentException {
    if (arg == null) {
      throw new IllegalArgumentException("Invalid null argument");
    } else if (arg.isEmpty()) {
      throw new IllegalArgumentException("Invalid empty argument");
    }
    switch (mode) {
    case ROUND_UP:
      if (arg.startsWith("+")) {
        min = (Long.parseLong(arg.substring(1)) * units) + 1L;
      } else if (arg.startsWith("-")) {
        max = (Long.parseLong(arg.substring(1)) * units) - units;
      } else {
        max = Long.parseLong(arg) * units;
        min = max - units + 1L;
      }
      break;
    case ROUND_DOWN:
      if (arg.startsWith("+")) {
        min = (Long.parseLong(arg.substring(1)) * units) + units;
      } else if (arg.startsWith("-")) {
        max = (Long.parseLong(arg.substring(1)) * units) - 1L;
      } else {
        min = Long.parseLong(arg) * units;
        max = min + units - 1L;
      }
      break;
    default:
      throw new AssertionError("Unexpected mode: " + mode);
    }
  }

  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  /**
   * Apply this {@link Expression} to the given value.
   *
   * @param value what to apply the expression to
   * @return {@link Result#PASS} is the value is within range,
   * {@link Result#FAIL} otherwise
   */
  protected Result applyNumber(long value) {
    if ((min > Long.MIN_VALUE) && (min > value)) {
      return Result.FAIL;
    }
    if ((max > Long.MIN_VALUE) && (max < value)) {
      return Result.FAIL;
    }
    return Result.PASS;
  }
}
