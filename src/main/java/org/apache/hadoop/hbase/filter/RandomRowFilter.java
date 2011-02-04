/**
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.KeyValue;

/**
 * A filter that includes rows based on a chance.
 * 
 */
public class RandomRowFilter extends FilterBase {
  protected static final Random random = new Random();

  protected float chance;
  protected boolean filterOutRow;

  /**
   * Writable constructor, do not use.
   */
  public RandomRowFilter() {
  }

  /**
   * Create a new filter with a specified chance for a row to be included.
   * 
   * @param chance
   */
  public RandomRowFilter(float chance) {
    this.chance = chance;
  }

  /**
   * @return The chance that a row gets included.
   */
  public float getChance() {
    return chance;
  }

  /**
   * Set the chance that a row is included.
   * 
   * @param chance
   */
  public void setChance(float chance) {
    this.chance = chance;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if (filterOutRow) {
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRow() {
    return filterOutRow;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (chance < 0) {
      // with a zero chance, the rows is always excluded
      filterOutRow = true;
    } else if (chance > 1) {
      // always included
      filterOutRow = false;
    } else {
      // roll the dice
      filterOutRow = !(random.nextFloat() < chance);
    }
    return filterOutRow;
  }

  @Override
  public void reset() {
    filterOutRow = false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    chance = in.readFloat();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(chance);
  }
}