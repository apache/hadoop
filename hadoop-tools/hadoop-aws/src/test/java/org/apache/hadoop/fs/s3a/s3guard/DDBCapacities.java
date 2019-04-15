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

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.Map;
import java.util.Objects;

import org.junit.Assert;

import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.READ_CAPACITY;

class DDBCapacities {
  private final long read, write;

  DDBCapacities(long read, long write) {
    this.read = read;
    this.write = write;
  }

  public long getRead() {
    return read;
  }

  public long getWrite() {
    return write;
  }

  String getReadStr() {
    return Long.toString(read);
  }

  String getWriteStr() {
    return Long.toString(write);
  }

  void checkEquals(String text, DDBCapacities that) throws Exception {
    if (!this.equals(that)) {
      throw new Exception(text + " expected = " + this +"; actual = "+ that);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DDBCapacities that = (DDBCapacities) o;
    return read == that.read && write == that.write;
  }

  @Override
  public int hashCode() {
    return Objects.hash(read, write);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Capacities{");
    sb.append("read=").append(read);
    sb.append(", write=").append(write);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Is the the capacity that of a pay-on-demand table?
   * @return true if the capacities are both 0.
   */
  public boolean isOnDemandTable() {
    return read == 0 && write == 0;
  }

  /**
   * Given a diagnostics map from a DDB store, extract the capacities.
   * @param diagnostics diagnostics map to examine.
   * @return the capacities
   * @throws AssertionError if the fields are missing.
   */
  public static DDBCapacities extractCapacities(
      final Map<String, String> diagnostics) {
    String read = diagnostics.get(READ_CAPACITY);
    Assert.assertNotNull("No " + READ_CAPACITY + " attribute in diagnostics",
        read);
    return new DDBCapacities(
        Long.parseLong(read),
        Long.parseLong(diagnostics.get(DynamoDBMetadataStore.WRITE_CAPACITY)));
  }

}
