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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

/**
 * A set of named counters.
 */
public class Counters implements Writable {
  
  private Map<String,Long> counters = new TreeMap<String,Long>();

  /**
   * Returns the names of all counters.
   * @return Set of counter names.
   */
  public synchronized Set<String> getCounterNames() {
    return counters.keySet();
  }
  
  /**
   * Returns the value of the named counter, or 0 if counter doesn't exist.
   * @param name name of a counter
   * @return value of the counter
   */
  public synchronized long getCounter(String name) {
    Long result = counters.get(name);
    return (result == null ? 0L : result);
  }
  
  /**
   * Increments the named counter by the specified amount, creating it if
   * it didn't already exist.
   * @param name of a counter
   * @param amount amount by which counter is to be incremented
   */
  public synchronized void incrCounter(String name, long amount) {
    counters.put(name, amount + getCounter(name));
  }
  
  /**
   * Increments multiple counters by their amounts in another Counters 
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for (String name : other.getCounterNames()) {
      incrCounter(name, other.getCounter(name));
    }
  }
  
  /**
   * Returns the number of counters.
   */
  public synchronized int size() {
	  return counters.size();
  }
  
  // Writable
  
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(counters.size());
    for (String name : counters.keySet()) {
      UTF8.writeString(out, name);
      out.writeLong(counters.get(name));
    }
  }
  
  public synchronized void readFields(DataInput in) throws IOException {
    int n = in.readInt();
    while (n-- > 0) {
      String name = UTF8.readString(in);
      long value = in.readLong();
      counters.put(name, value);
    }
  }
  
  /**
   * Logs the current counter values.
   * @param log The log to use.
   */
  public void log(Log log) {
    log.info("Counters: " + getCounterNames().size());
    for (String counterName : getCounterNames()) {
      log.info("  " + counterName + "=" + getCounter(counterName));
    }
  }
  
}
