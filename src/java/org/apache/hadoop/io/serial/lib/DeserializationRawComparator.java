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

package org.apache.hadoop.io.serial.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.Serialization;

/**
 * <p>
 * A {@link RawComparator} that uses a {@link Serialization}
 * object to deserialize objects that are then compared via
 * their {@link Comparable} interfaces.
 * </p>
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DeserializationRawComparator<T extends Comparable<T>> 
  implements RawComparator {
  private final Serialization<T> serialization;
  private final Configuration conf;

  private static final class ReusableObjects<T extends Comparable<T>> {
    DataInputBuffer buf = new DataInputBuffer();
    T left = null;
    T right = null;
  }

  private static final ThreadLocal<ReusableObjects<?>> REUSE_FACTORY =
    new ThreadLocal<ReusableObjects<?>>(){
    @SuppressWarnings("unchecked")
    @Override
    protected ReusableObjects<?> initialValue() {
      return new ReusableObjects();
    }
  };

  public DeserializationRawComparator(Serialization<T> serialization,
                                     Configuration conf) {
    this.serialization = serialization;
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    ReusableObjects<T> reuse = (ReusableObjects<T>) REUSE_FACTORY.get();
    try {
      reuse.buf.reset(b1, s1, l1);
      reuse.left = serialization.deserialize(reuse.buf, reuse.left, conf);
      reuse.buf.reset(b2, s2, l2);
      reuse.right = serialization.deserialize(reuse.buf, reuse.right, conf);
      return reuse.left.compareTo(reuse.right);
    } catch (IOException e) {
      throw new RuntimeException("Error in deserialization",e);
    }
  }

}
