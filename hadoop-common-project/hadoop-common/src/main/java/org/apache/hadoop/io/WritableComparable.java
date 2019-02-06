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

package org.apache.hadoop.io;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A {@link Writable} which is also {@link Comparable}. 
 *
 * <p><code>WritableComparable</code>s can be compared to each other, typically 
 * via <code>Comparator</code>s. Any type which is to be used as a 
 * <code>key</code> in the Hadoop Map-Reduce framework should implement this
 * interface.</p>
 *
 * <p>Note that <code>hashCode()</code> is frequently used in Hadoop to partition
 * keys. It's important that your implementation of hashCode() returns the same 
 * result across different instances of the JVM. Note also that the default 
 * <code>hashCode()</code> implementation in <code>Object</code> does <b>not</b>
 * satisfy this property.</p>
 *  
 * <p>Example:</p>
 * <blockquote><pre>
 *     public class MyWritableComparable implements
 *      WritableComparable{@literal <MyWritableComparable>} {
 *       // Some data
 *       private int counter;
 *       private long timestamp;
 *       
 *       public void write(DataOutput out) throws IOException {
 *         out.writeInt(counter);
 *         out.writeLong(timestamp);
 *       }
 *       
 *       public void readFields(DataInput in) throws IOException {
 *         counter = in.readInt();
 *         timestamp = in.readLong();
 *       }
 *       
 *       public int compareTo(MyWritableComparable o) {
 *         int thisValue = this.value;
 *         int thatValue = o.value;
 *         return (thisValue &lt; thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
 *       }
 *
 *       public int hashCode() {
 *         final int prime = 31;
 *         int result = 1;
 *         result = prime * result + counter;
 *         result = prime * result + (int) (timestamp ^ (timestamp &gt;&gt;&gt; 32));
 *         return result
 *       }
 *     }
 * </pre></blockquote>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface WritableComparable<T> extends Writable, Comparable<T> {
}
