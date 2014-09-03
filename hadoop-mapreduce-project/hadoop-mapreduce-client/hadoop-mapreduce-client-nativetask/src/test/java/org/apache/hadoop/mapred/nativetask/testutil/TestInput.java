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
package org.apache.hadoop.mapred.nativetask.testutil;

import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestInput {

  public static class KV<K, V> {
    public K key;
    public V value;
  }

  public static char[] CHAR_SET = new char[] {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q',
    'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
    'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
    'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '*', '/' };

  public static KV[] getMapInputs(int size) {

    final KV[] dataInput = new KV[size];

    for (int i = 0; i < size; i++) {
      dataInput[i] = getSingleMapInput(i);
    }
    return dataInput;
  }

  private static KV getSingleMapInput(int i) {
    final char character = CHAR_SET[i % CHAR_SET.length];
    final byte b = (byte) character;

    final byte[] bytes = new byte[i];
    Arrays.fill(bytes, b);
    final BytesWritable result = new BytesWritable(bytes);
    final KV kv = new KV();
    kv.key = result;
    kv.value = result;
    return kv;
  }

}
