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

package org.apache.hadoop.io.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * A {@link RawComparator} that uses Avro to extract data from the
 * source stream and compare their contents without explicit
 * deserialization.
 */
public class AvroComparator<T extends Comparable<T>>
    implements RawComparator<T> {

  private final Schema schema;

  public AvroComparator(final Schema s) {
    this.schema = s;
  }

  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return BinaryData.compare(b1, s1, b2, s2, schema);
  }

  public int compare(T t1, T t2) {
    return t1.compareTo(t2);
  }

}
