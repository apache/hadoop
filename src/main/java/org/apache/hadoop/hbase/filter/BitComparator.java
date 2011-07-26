/*
 * Copyright 2010 The Apache Software Foundation
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

/**
 * A bit comparator which performs the specified bitwise operation on each of the bytes
 * with the specified byte array. Then returns whether the result is non-zero.
 */
public class BitComparator extends WritableByteArrayComparable {

  /** Nullary constructor for Writable, do not use */
  public BitComparator() {}

  /** Bit operators. */
  public enum BitwiseOp {
    /** and */
    AND,
    /** or */
    OR,
    /** xor */
    XOR
  }
  protected BitwiseOp bitOperator;

  /**
   * Constructor
   * @param value value
   * @param BitwiseOp bitOperator - the operator to use on the bit comparison
   */
  public BitComparator(byte[] value, BitwiseOp bitOperator) {
    super(value);
    this.bitOperator = bitOperator;
  }

  /**
   * @return the bitwise operator
   */
  public BitwiseOp getOperator() {
    return bitOperator;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    bitOperator = BitwiseOp.valueOf(in.readUTF());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(bitOperator.name());
  }

  @Override
  public int compareTo(byte[] value) {
    if (value.length != this.value.length) {
      return 1;
    }
    int b = 0;
    //Iterating backwards is faster because we can quit after one non-zero byte.
    for (int i = value.length - 1; i >= 0 && b == 0; i--) {
      switch (bitOperator) {
        case AND:
          b = (this.value[i] & value[i]) & 0xff;
          break;
        case OR:
          b = (this.value[i] | value[i]) & 0xff;
          break;
        case XOR:
          b = (this.value[i] ^ value[i]) & 0xff;
          break;
      }
    }
    return b == 0 ? 1 : 0;
  }
}

