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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A set of named counters.
 * 
 * <p><code>Counters</code> represent global counters, defined either by the 
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 * 
 * <p><code>Counters</code> are bunched into {@link Group}s, each comprising of
 * counters from a particular <code>Enum</code> class. 
 */
public class Counters implements Writable, Iterable<Counters.Group> {
  private static final Log LOG = LogFactory.getLog(Counters.class);
  
  //private static Log log = LogFactory.getLog("Counters.class");
  
  /**
   * A counter record, comprising its name and value. 
   */
  public static class Counter implements Writable {

    private String name;
    private String displayName;
    private long value;
    
    Counter() { 
      value = 0L;
    }

    Counter(String name, String displayName, long value) {
      this.name = name;
      this.displayName = displayName;
      this.value = value;
    }
    
    /**
     * Read the binary representation of the counter
     */
    public synchronized void readFields(DataInput in) throws IOException {
      name = Text.readString(in);
      if (in.readBoolean()) {
        displayName = Text.readString(in);
      } else {
        displayName = name;
      }
      value = WritableUtils.readVLong(in);
    }
    
    /**
     * Write the binary representation of the counter
     */
    public synchronized void write(DataOutput out) throws IOException {
      Text.writeString(out, name);
      boolean distinctDisplayName = (! name.equals(displayName));
      out.writeBoolean(distinctDisplayName);
      if (distinctDisplayName) {
        Text.writeString(out, displayName);
      }
      WritableUtils.writeVLong(out, value);
    }
    
    /**
     * Get the internal name of the counter.
     * @return the internal name of the counter
     */
    public synchronized String getName() {
      return name;
    }
    
    /**
     * Get the name of the counter.
     * @return the user facing name of the counter
     */
    public synchronized String getDisplayName() {
      return displayName;
    }
    
    /**
     * What is the current value of this counter?
     * @return the current value
     */
    public synchronized long getCounter() {
      return value;
    }
    
    /**
     * Increment this counter by the given value
     * @param incr the value to increase this counter by
     */
    public synchronized void increment(long incr) {
      value += incr;
    }
  }
  
  /**
   *  <code>Group</code> of counters, comprising of counters from a particular 
   *  counter {@link Enum} class.  
   *
   *  <p><code>Group</code>handles localization of the class name and the 
   *  counter names.</p>
   */
  public static class Group implements Writable, Iterable<Counter> {
    private String groupName;
    private String displayName;
    private Map<String, Counter> subcounters = new HashMap<String, Counter>();
    
    // Optional ResourceBundle for localization of group and counter names.
    private ResourceBundle bundle = null;    
    
    Group(String groupName) {
      try {
        bundle = getResourceBundle(groupName);
      }
      catch (MissingResourceException neverMind) {
      }
      this.groupName = groupName;
      this.displayName = localize("CounterGroupName", groupName);
      LOG.debug("Creating group " + groupName + " with " +
               (bundle == null ? "nothing" : "bundle"));
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
      return displayName;
    }
    
    /**
     * Returns the value of the specified counter, or 0 if the counter does
     * not exist.
     */
    public synchronized long getCounter(String counterName) {
      for(Counter counter: subcounters.values()) {
        if (counter != null && counter.displayName.equals(counterName)) {
          return counter.value;
        }
      }
      return 0L;
    }
    
    /**
     * Get the counter for the given id and create it if it doesn't exist.
     * @param id the numeric id of the counter within the group
     * @param name the internal counter name
     * @return the counter
     * @deprecated use {@link #getCounter(String)} instead
     */
    @Deprecated
    public synchronized Counter getCounter(int id, String name) {
      return getCounterForName(name);
    }
    
    /**
     * Get the counter for the given name and create it if it doesn't exist.
     * @param name the internal counter name
     * @return the counter
     */
    public synchronized Counter getCounterForName(String name) {
      Counter result = subcounters.get(name);
      if (result == null) {
        LOG.debug("Adding " + name);
        result = new Counter(name, localize(name + ".name", name), 0L);
        subcounters.put(name, result);
      }
      return result;
    }
    
    /**
     * Returns the number of counters in this group.
     */
    public synchronized int size() {
      return subcounters.size();
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
    
    public synchronized void write(DataOutput out) throws IOException {
      Text.writeString(out, displayName);
      WritableUtils.writeVInt(out, subcounters.size());
      for(Counter counter: subcounters.values()) {
        counter.write(out);
      }
    }
    
    public synchronized void readFields(DataInput in) throws IOException {
      displayName = Text.readString(in);
      subcounters.clear();
      int size = WritableUtils.readVInt(in);
      for(int i=0; i < size; i++) {
        Counter counter = new Counter();
        counter.readFields(in);
        subcounters.put(counter.getName(), counter);
      }
    }

    public synchronized Iterator<Counter> iterator() {
      return new ArrayList<Counter>(subcounters.values()).iterator();
    }
  }
  
  // Map from group name (enum class name) to map of int (enum ordinal) to
  // counter record (name-value pair).
  private Map<String,Group> counters = new HashMap<String, Group>();

  /**
   * A cache from enum values to the associated counter. Dramatically speeds up
   * typical usage.
   */
  @SuppressWarnings("unchecked")
  private Map<Enum, Counter> cache = new IdentityHashMap<Enum, Counter>();
  
  /**
   * Returns the names of all counter classes.
   * @return Set of counter names.
   */
  public synchronized Collection<String> getGroupNames() {
    return counters.keySet();
  }

  public synchronized Iterator<Group> iterator() {
    return counters.values().iterator();
  }

  /**
   * Returns the named counter group, or an empty group if there is none
   * with the specified name.
   */
  public synchronized Group getGroup(String groupName) {
    Group result = counters.get(groupName);
    if (result == null) {
      result = new Group(groupName);
      counters.put(groupName, result);
    }
    return result;
  }

  /**
   * Find the counter for the given enum. The same enum will always return the
   * same counter.
   * @param key the counter key
   * @return the matching counter object
   */
  @SuppressWarnings("unchecked")
  public synchronized Counter findCounter(Enum key) {
    Counter counter = cache.get(key);
    if (counter == null) {
      Group group = getGroup(key.getDeclaringClass().getName());
      counter = group.getCounterForName(key.toString());
      cache.put(key, counter);
    }
    return counter;    
  }

  /**
   * Find a counter by using strings
   * @param group the name of the group
   * @param id the id of the counter within the group (0 to N-1)
   * @param name the internal name of the counter
   * @return the counter for that name
   * @deprecated
   */
  @Deprecated
  public synchronized Counter findCounter(String group, int id, String name) {
    return getGroup(group).getCounterForName(name);
  }

  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param key identifies a counter
   * @param amount amount by which counter is to be incremented
   */
  @SuppressWarnings("unchecked")
  public synchronized void incrCounter(Enum key, long amount) {
    findCounter(key).value += amount;
  }
  
  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param group the name of the group
   * @param counter the internal name of the counter
   * @param amount amount by which counter is to be incremented
   */
  @SuppressWarnings("unchecked")
  public synchronized void incrCounter(String group, String counter, long amount) {
    getGroup(group).getCounterForName(counter).value += amount;
  }
  
  /**
   * Returns current value of the specified counter, or 0 if the counter
   * does not exist.
   */
  @SuppressWarnings("unchecked")
  public synchronized long getCounter(Enum key) {
    return findCounter(key).value;
  }
  
  /**
   * Increments multiple counters by their amounts in another Counters 
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for (Group otherGroup: other) {
      Group group = getGroup(otherGroup.getName());
      group.displayName = otherGroup.displayName;
      for (Counter otherCounter : otherGroup) {
        Counter counter = group.getCounterForName(otherCounter.getName());
        counter.displayName = otherCounter.displayName;
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
    for (Group group : this) {
      result += group.size();
    }
    return result;
  }
  
  /**
   * Write the set of groups.
   * The external format is:
   *     #groups (groupName group)*
   *
   * i.e. the number of groups followed by 0 or more groups, where each 
   * group is of the form:
   *
   *     groupDisplayName #counters (false | true counter)*
   *
   * where each counter is of the form:
   *
   *     name (false | true displayName) value
   */
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(counters.size());
    for (Group group: counters.values()) {
      Text.writeString(out, group.getName());
      group.write(out);
    }
  }
  
  /**
   * Read a set of groups.
   */
  public synchronized void readFields(DataInput in) throws IOException {
    int numClasses = in.readInt();
    counters.clear();
    while (numClasses-- > 0) {
      String groupName = Text.readString(in);
      Group group = new Group(groupName);
      group.readFields(in);
      counters.put(groupName, group);
    }
  }
  
  /**
   * Logs the current counter values.
   * @param log The log to use.
   */
  public void log(Log log) {
    log.info("Counters: " + size());
    for(Group group: this) {
      log.info("  " + group.getDisplayName());
      for (Counter counter: group) {
        log.info("    " + counter.getDisplayName() + "=" + 
                 counter.getCounter());
      }   
    }
  }
  
  /**
   * Return textual representation of the counter values.
   */
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("Counters: " + size());
    for (Group group: this) {
      sb.append("\n\t" + group.getDisplayName());
      for (Counter counter: group) {
        sb.append("\n\t\t" + counter.getDisplayName() + "=" + 
                  counter.getCounter());
      }
    }
    return sb.toString();
  }

  /**
   * Convert a counters object into a single line that is easy to parse.
   * @return the string with "name=value" for each counter and separated by ","
   */
  public synchronized String makeCompactString() {
    StringBuffer buffer = new StringBuffer();
    boolean first = true;
    for(Group group: this){   
      for(Counter counter: group) {
        if (first) {
          first = false;
        } else {
          buffer.append(',');
        }
        buffer.append(group.getDisplayName());
        buffer.append('.');
        buffer.append(counter.getDisplayName());
        buffer.append(':');
        buffer.append(counter.getCounter());
      }
    }
    return buffer.toString();
  }
}
