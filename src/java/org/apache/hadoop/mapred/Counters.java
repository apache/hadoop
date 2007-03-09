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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeMap;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

/**
 * A set of named counters.
 */
public class Counters implements Writable {
  
  //private static Log log = LogFactory.getLog("Counters.class");
  
  /**
   * A counter record, comprising its name and value. 
   */
  private static class CounterRec {
    
    public String name;
    public long value;
    
    public CounterRec(String name, long value) {
      this.name = name;
      this.value = value;
    }
    
  } // end class CounterRec
  
  /**
   *  Represents a group of counters, comprising the counters from a particular 
   *  counter enum class.  
   *
   *  This class handles localization of the class name and the counter names.
   */
  public static class Group {
    
    // The group name is the fully qualified enum class name. 
    private String groupName;
    
    // Optional ResourceBundle for localization of group and counter names.
    private ResourceBundle bundle = null;
    
    // Maps counter names to their current values.  Note that the iteration
    // order of this Map is the same as the ordering of the Enum class in which 
    // these counter names were defined.
    private Map<String,Long> groupCounters = new LinkedHashMap<String,Long>();
    
    
    Group(String groupName, Collection<CounterRec> counters) {
      this.groupName = groupName;
      try {
        bundle = getResourceBundle(groupName);
      }
      catch (MissingResourceException neverMind) {
      }
      
      for (CounterRec counter : counters) {
        groupCounters.put(counter.name, counter.value);
      }
    }
    
    /**
     * Returns the specified resource bundle, or throws an exception.
     * @throws MissingResourceException if the bundle isn't found
     */
    private static ResourceBundle getResourceBundle(String enumClassName) {
      String bundleName = enumClassName.replace('$','_');
      return ResourceBundle.getBundle(bundleName);
    }
    
    /**
     * Returns raw name of the group.  This is the name of the enum class
     * for this group of counters.
     */
    public String getName() {
      return groupName;
    }
    
    /**
     * Returns localized name of the group.  This is the same as getName() by
     * default, but different if an appropriate ResourceBundle is found.
     */
    public String getDisplayName() {
      return localize("CounterGroupName", groupName);
    }
    
    /**
     * Returns localized name of the specified counter.
     */
    public String getDisplayName(String counter) {
      return localize(counter + ".name", counter);
    }
    
    /**
     * Returns the counters for this group, with their names localized.
     */
    public Collection<String> getCounterNames() {
      return groupCounters.keySet();
    }
    
    /**
     * Returns the value of the specified counter, or 0 if the counter does
     * not exist.
     */
    public long getCounter(String counter) {
      Long result = groupCounters.get(counter);
      return (result == null ? 0L : result);
    }
    
    /**
     * Returns the number of counters in this group.
     */
    public int size() {
      return groupCounters.size();
    }
    
        /**
     * Looks up key in the ResourceBundle and returns the corresponding value.
     * If the bundle or the key doesn't exist, returns the default value.
     */
    private String localize(String key, String defaultValue) {
      String result = defaultValue;
      if (bundle != null) {
        try {
          result = bundle.getString(key);
        }
        catch (MissingResourceException mre) {
        }
      }
      return result;
    }
    
    
  } // end class Group
  
  
  // Map from group name (enum class name) to map of int (enum ordinal) to
  // counter record (name-value pair).
  private Map<String,Map<Integer,CounterRec>> counters =
          new TreeMap<String,Map<Integer,CounterRec>>();
  
  /**
   * Returns the names of all counter classes.
   * @return Set of counter names.
   */
  public synchronized Collection<String> getGroupNames() {
    return new ArrayList<String>(counters.keySet());
  }
  
  /**
   * Returns the named counter group, or an empty group if there is none
   * with the specified name.
   */
  public synchronized Group getGroup(String groupName) {
    Map<Integer,CounterRec> counterMap = counters.get(groupName);
    Collection<CounterRec> groupCounters;
    if (counterMap == null) {
      groupCounters = Collections.emptySet();
    }
    else {
      groupCounters = counterMap.values();
    }
    return new Group(groupName, groupCounters);
  }
  
  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param key identifies a counter
   * @param amount amount by which counter is to be incremented
   */
  public synchronized void incrCounter(Enum key, long amount) {
    int ordinal = key.ordinal();
    String counterName = key.toString();
    String groupName = key.getDeclaringClass().getName();
    Map<Integer,CounterRec> counterMap = getCounterMap(groupName);
    CounterRec counter = getCounter(counterMap, counterName, ordinal);
    counter.value += amount;
  }
  
  /**
   * Returns current value of the specified counter, or 0 if the counter
   * does not exist.
   */
  public synchronized long getCounter(Enum key) {
    long result = 0L;
    String groupName = key.getDeclaringClass().getName();
    Map<Integer,CounterRec> counterMap = counters.get(groupName);
    if (counterMap != null) {
      int ordinal = key.ordinal();
      String name = key.toString();
      CounterRec counter = counterMap.get(ordinal);
      if (counter != null && counter.name.equals(name)) {
        result = counter.value;
      }
      else {
        // ordinal lookup failed, but maybe there is a matching name; this 
        // could happen if e.g. a client has a different version of the Enum class.
        for (CounterRec ctr : counterMap.values()) {
          if (ctr.name.equals(name)) {
            result = ctr.value;
            break;
          }
        }
      }
    }
    return result;
  }
  
  /**
   * Returns the counters for the specified counter class. The counters are
   * returned as a map from ordinal number, so that their ordering in the 
   * enum class declaration is preserved.
   */
  private Map<Integer,CounterRec> getCounterMap(String groupName) {
    Map<Integer,CounterRec> map = counters.get(groupName);
    if (map == null) {
      map = new TreeMap<Integer,CounterRec>();
      counters.put(groupName, map);
    }
    return map;
  }

  /**
   * Returns the counter record with the specified name and ordinal by 
   * finding or creating it in the specified counterMap.
   */
  private CounterRec getCounter(Map<Integer,CounterRec> counterMap, 
                                String counterName, int ordinal)
  {
    CounterRec result = counterMap.get(ordinal);
    if (result == null) {
      result = new CounterRec(counterName, 0L);
      counterMap.put(ordinal, result);
    }
    return result;
  }
  
  /**
   * Increments multiple counters by their amounts in another Counters 
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for (String groupName : other.counters.keySet()) {
      Map<Integer,CounterRec> otherCounters = other.counters.get(groupName);
      Map<Integer,CounterRec> myCounters = getCounterMap(groupName);
      for (int i : otherCounters.keySet()) {
        CounterRec otherCounter = otherCounters.get(i);
        CounterRec counter = getCounter(myCounters, otherCounter.name, i);
        counter.value += otherCounter.value;
      }
    }
  }

  /**
   * Convenience method for computing the sum of two sets of counters.
   */
  public static Counters sum(Counters a, Counters b) {
      Counters counters = new Counters();
      counters.incrAllCounters(a);
      counters.incrAllCounters(b);
      return counters;
    }
  
  /**
   * Returns the total number of counters, by summing the number of counters
   * in each group.
   */
  public synchronized  int size() {
    int result = 0;
    for (String groupName : counters.keySet()) {
      result += counters.get(groupName).size();
    }
    return result;
  }
  
  // Writable.  The external format is:
  //  
  //     #groups group*
  //
  // i.e. the number of groups followed by 0 or more groups, where each 
  // group is of the form:
  //
  //     groupName #counters counter*
  //
  // where each counter is of the form:
  //
  //     ordinal name value
  //
  
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(counters.size());
    for (String groupName : counters.keySet()) {
      UTF8.writeString(out, groupName);
      Map<Integer,CounterRec> map = counters.get(groupName);
      out.writeInt(map.size());
      for (Integer ordinal : map.keySet()) {
        CounterRec counter = map.get(ordinal);
        out.writeInt(ordinal);
        UTF8.writeString(out, counter.name);
        out.writeLong(counter.value);
      }
    }
  }
  
  public synchronized void readFields(DataInput in) throws IOException {
    int numClasses = in.readInt();
    while (numClasses-- > 0) {
      String groupName = UTF8.readString(in);
      Map<Integer,CounterRec> counters = getCounterMap(groupName);
      int numCounters = in.readInt();
      while (numCounters-- > 0) {
        int index = in.readInt();
        String counterName = UTF8.readString(in);
        long value = in.readLong();
        counters.put(index, new CounterRec(counterName,value));
      }
    }
  }
  
  /**
   * Logs the current counter values.
   * @param log The log to use.
   */
  public void log(Log log) {
    log.info("Counters: " + size());
    Collection<String> groupNames = getGroupNames();
    for (String groupName : groupNames) {
      Group group = getGroup(groupName);
      log.info("  " + group.getDisplayName());
      for (String counterName : group.getCounterNames()) {
        log.info("    " + group.getDisplayName(counterName) + "=" + 
                 group.getCounter(counterName));
      }
    }
  }

  
}
