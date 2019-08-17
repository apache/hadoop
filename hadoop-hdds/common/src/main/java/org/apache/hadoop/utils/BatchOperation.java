/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.utils;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * An utility class to store a batch of DB write operations.
 */
public class BatchOperation {

  /**
   * Enum for write operations.
   */
  public enum Operation {
    DELETE, PUT
  }

  private List<SingleOperation> operations =
      Lists.newArrayList();

  /**
   * Add a PUT operation into the batch.
   */
  public void put(byte[] key, byte[] value) {
    operations.add(new SingleOperation(Operation.PUT, key, value));
  }

  /**
   * Add a DELETE operation into the batch.
   */
  public void delete(byte[] key) {
    operations.add(new SingleOperation(Operation.DELETE, key, null));

  }

  public List<SingleOperation> getOperations() {
    return operations;
  }

  /**
   * A SingleOperation represents a PUT or DELETE operation
   * and the data the operation needs to manipulates.
   */
  public static class SingleOperation {

    private Operation opt;
    private byte[] key;
    private byte[] value;

    public SingleOperation(Operation opt, byte[] key, byte[] value) {
      this.opt = opt;
      if (key == null) {
        throw new IllegalArgumentException("key cannot be null");
      }
      this.key = key.clone();
      this.value = value == null ? null : value.clone();
    }

    public Operation getOpt() {
      return opt;
    }

    public byte[] getKey() {
      return key.clone();
    }

    public byte[] getValue() {
      return value == null ? null : value.clone();
    }
  }
}
