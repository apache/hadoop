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

package org.apache.hadoop.tools.rumen;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.annotate.JsonAnySetter;

// HACK ALERT!!!  This "should" have have two subclasses, which might be called
//                LoggedMapTaskAttempt and LoggedReduceTaskAttempt, but 
//                the Jackson implementation of JSON doesn't handle a 
//                superclass-valued field.

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.jobhistory.JhCounter;
import org.apache.hadoop.mapreduce.jobhistory.JhCounterGroup;
import org.apache.hadoop.mapreduce.jobhistory.JhCounters;
import org.apache.hadoop.tools.rumen.datatypes.NodeName;

/**
 * A {@link LoggedTaskAttempt} represents an attempt to run an hadoop task in a
 * hadoop job. Note that a task can have several attempts.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedTaskAttempt implements DeepCompare {

  TaskAttemptID attemptID;
  Pre21JobHistoryConstants.Values result;
  long startTime = -1L;
  long finishTime = -1L;
  NodeName hostName;

  long hdfsBytesRead = -1L;
  long hdfsBytesWritten = -1L;
  long fileBytesRead = -1L;
  long fileBytesWritten = -1L;
  long mapInputRecords = -1L;
  long mapInputBytes = -1L;
  long mapOutputBytes = -1L;
  long mapOutputRecords = -1L;
  long combineInputRecords = -1L;
  long reduceInputGroups = -1L;
  long reduceInputRecords = -1L;
  long reduceShuffleBytes = -1L;
  long reduceOutputRecords = -1L;
  long spilledRecords = -1L;

  long shuffleFinished = -1L;
  long sortFinished = -1L;

  LoggedLocation location;

  // Initialize to default object for backward compatibility
  ResourceUsageMetrics metrics = new ResourceUsageMetrics();
  
  List<Integer> clockSplits = new ArrayList<Integer>();
  List<Integer> cpuUsages = new ArrayList<Integer>();
  List<Integer> vMemKbytes = new ArrayList<Integer>();
  List<Integer> physMemKbytes = new ArrayList<Integer>();

  LoggedTaskAttempt() {
    super();
  }

  // carries the kinds of splits vectors a LoggedTaskAttempt holds.
  //
  // Each enumeral has the following methods:
  //   get(LoggedTaskAttempt attempt)
  //    returns a List<Integer> with the corresponding value field
  //   set(LoggedTaskAttempt attempt, List<Integer> newValue)
  //    sets the value
  // There is also a pair of methods get(List<List<Integer>>) and
  //  set(List<List<Integer>>, List<Integer>) which correspondingly
  //  delivers or sets the appropriate element of the
  //  List<List<Integer>> .
  // This makes it easier to add another kind in the future.
  public enum SplitVectorKind {

    WALLCLOCK_TIME {
      @Override
      public List<Integer> get(LoggedTaskAttempt attempt) {
        return attempt.getClockSplits();
      }
      @Override
      public void set(LoggedTaskAttempt attempt, List<Integer> newValue) {
        attempt.setClockSplits(newValue);
      }
    },

    CPU_USAGE {
      @Override
      public List<Integer> get(LoggedTaskAttempt attempt) {
        return attempt.getCpuUsages();
      }
      @Override
      public void set(LoggedTaskAttempt attempt, List<Integer> newValue) {
        attempt.setCpuUsages(newValue);
      }
    },

    VIRTUAL_MEMORY_KBYTES {
      @Override
      public List<Integer> get(LoggedTaskAttempt attempt) {
        return attempt.getVMemKbytes();
      }
      @Override
      public void set(LoggedTaskAttempt attempt, List<Integer> newValue) {
        attempt.setVMemKbytes(newValue);
      }
    },

    PHYSICAL_MEMORY_KBYTES {
      @Override
      public List<Integer> get(LoggedTaskAttempt attempt) {
        return attempt.getPhysMemKbytes();
      }
      @Override
      public void set(LoggedTaskAttempt attempt, List<Integer> newValue) {
        attempt.setPhysMemKbytes(newValue);
      }
    };

    static private final List<List<Integer>> NULL_SPLITS_VECTOR
      = new ArrayList<List<Integer>>();

    static {
      for (SplitVectorKind kind : SplitVectorKind.values() ) {
        NULL_SPLITS_VECTOR.add(new ArrayList<Integer>());
      }
    }

    abstract public List<Integer> get(LoggedTaskAttempt attempt);

    abstract public void set(LoggedTaskAttempt attempt, List<Integer> newValue);

    public List<Integer> get(List<List<Integer>> listSplits) {
      return listSplits.get(this.ordinal());
    }

    public void set(List<List<Integer>> listSplits, List<Integer> newValue) {
      listSplits.set(this.ordinal(), newValue);
    }

    static public List<List<Integer>> getNullSplitsVector() {
      return NULL_SPLITS_VECTOR;
    }
  }

  /**
   *
   * @return a list of all splits vectors, ordered in enumeral order
   *           within {@link SplitVectorKind} .  Do NOT use hard-coded
   *           indices within the return for this with a hard-coded
   *           index to get individual values; use
   *           {@code SplitVectorKind.get(LoggedTaskAttempt)} instead.
   */
  public List<List<Integer>> allSplitVectors() {
    List<List<Integer>> result
      = new ArrayList<List<Integer>>(SplitVectorKind.values().length);

    for (SplitVectorKind kind : SplitVectorKind.values() ) {
      result.add(kind.get(this));
    }

    return result;
  }

  static private Set<String> alreadySeenAnySetterAttributes =
      new TreeSet<String>();

  // for input parameter ignored.
  @JsonAnySetter
  public void setUnknownAttribute(String attributeName, Object ignored) {
    if (!alreadySeenAnySetterAttributes.contains(attributeName)) {
      alreadySeenAnySetterAttributes.add(attributeName);
      System.err.println("In LoggedJob, we saw the unknown attribute "
          + attributeName + ".");
    }
  }

  public List<Integer> getClockSplits() {
    return clockSplits;
  }

  void setClockSplits(List<Integer> clockSplits) {
    this.clockSplits = clockSplits;
  }

  void arraySetClockSplits(int[] clockSplits) {
    List<Integer> result = new ArrayList<Integer>();

    for (int i = 0; i < clockSplits.length; ++i) {
      result.add(clockSplits[i]);
    }

    this.clockSplits = result;
  }

  public List<Integer> getCpuUsages() {
    return cpuUsages;
  }

  void setCpuUsages(List<Integer> cpuUsages) {
    this.cpuUsages = cpuUsages;
  }

  void arraySetCpuUsages(int[] cpuUsages) {
    List<Integer> result = new ArrayList<Integer>();

    for (int i = 0; i < cpuUsages.length; ++i) {
      result.add(cpuUsages[i]);
    }

    this.cpuUsages = result;
  }

  public List<Integer> getVMemKbytes() {
    return vMemKbytes;
  }

  void setVMemKbytes(List<Integer> vMemKbytes) {
    this.vMemKbytes = vMemKbytes;
  }

  void arraySetVMemKbytes(int[] vMemKbytes) {
    List<Integer> result = new ArrayList<Integer>();

    for (int i = 0; i < vMemKbytes.length; ++i) {
      result.add(vMemKbytes[i]);
    }

    this.vMemKbytes = result;
  }

  public List<Integer> getPhysMemKbytes() {
    return physMemKbytes;
  }

  void setPhysMemKbytes(List<Integer> physMemKbytes) {
    this.physMemKbytes = physMemKbytes;
  }

  void arraySetPhysMemKbytes(int[] physMemKbytes) {
    List<Integer> result = new ArrayList<Integer>();

    for (int i = 0; i < physMemKbytes.length; ++i) {
      result.add(physMemKbytes[i]);
    }

    this.physMemKbytes = result;
  }

  void adjustTimes(long adjustment) {
    startTime += adjustment;
    finishTime += adjustment;

    // For reduce attempts, adjust the different phases' finish times also 
    if (sortFinished >= 0) {
      shuffleFinished += adjustment;
      sortFinished += adjustment;
    }
  }

  public long getShuffleFinished() {
    return shuffleFinished;
  }

  void setShuffleFinished(long shuffleFinished) {
    this.shuffleFinished = shuffleFinished;
  }

  public long getSortFinished() {
    return sortFinished;
  }

  void setSortFinished(long sortFinished) {
    this.sortFinished = sortFinished;
  }

  public TaskAttemptID getAttemptID() {
    return attemptID;
  }

  void setAttemptID(String attemptID) {
    this.attemptID = TaskAttemptID.forName(attemptID);
  }

  public Pre21JobHistoryConstants.Values getResult() {
    return result;
  }

  void setResult(Pre21JobHistoryConstants.Values result) {
    this.result = result;
  }

  public long getStartTime() {
    return startTime;
  }

  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public NodeName getHostName() {
    return hostName;
  }

  // This is needed for JSON deserialization
  void setHostName(String hostName) {
    this.hostName = hostName == null ? null : new NodeName(hostName);
  }

  // In job-history, hostName is saved in the format rackName/NodeName
  //TODO this is a hack! The '/' handling needs fixing.
  void setHostName(String hostName, String rackName) {
    if (hostName == null || hostName.length() == 0) {
      throw new RuntimeException("Invalid entry! Missing hostname");
    } else if (rackName == null || rackName.length() == 0) {
      setHostName(hostName);
    } else {
      // make sure that the rackname is prefixed with a '/'
      if (!rackName.startsWith("/")) {
        rackName = "/" + rackName;
      }
      // make sure that the hostname is prefixed with a '/'
      if (!hostName.startsWith("/")) {
        hostName = "/" + hostName;
      }
      setHostName(rackName.intern() + hostName.intern());
    }
  }

  public long getHdfsBytesRead() {
    return hdfsBytesRead;
  }

  void setHdfsBytesRead(long hdfsBytesRead) {
    this.hdfsBytesRead = hdfsBytesRead;
  }

  public long getHdfsBytesWritten() {
    return hdfsBytesWritten;
  }

  void setHdfsBytesWritten(long hdfsBytesWritten) {
    this.hdfsBytesWritten = hdfsBytesWritten;
  }

  public long getFileBytesRead() {
    return fileBytesRead;
  }

  void setFileBytesRead(long fileBytesRead) {
    this.fileBytesRead = fileBytesRead;
  }

  public long getFileBytesWritten() {
    return fileBytesWritten;
  }

  void setFileBytesWritten(long fileBytesWritten) {
    this.fileBytesWritten = fileBytesWritten;
  }

  public long getMapInputRecords() {
    return mapInputRecords;
  }

  void setMapInputRecords(long mapInputRecords) {
    this.mapInputRecords = mapInputRecords;
  }

  public long getMapOutputBytes() {
    return mapOutputBytes;
  }

  void setMapOutputBytes(long mapOutputBytes) {
    this.mapOutputBytes = mapOutputBytes;
  }

  public long getMapOutputRecords() {
    return mapOutputRecords;
  }

  void setMapOutputRecords(long mapOutputRecords) {
    this.mapOutputRecords = mapOutputRecords;
  }

  public long getCombineInputRecords() {
    return combineInputRecords;
  }

  void setCombineInputRecords(long combineInputRecords) {
    this.combineInputRecords = combineInputRecords;
  }

  public long getReduceInputGroups() {
    return reduceInputGroups;
  }

  void setReduceInputGroups(long reduceInputGroups) {
    this.reduceInputGroups = reduceInputGroups;
  }

  public long getReduceInputRecords() {
    return reduceInputRecords;
  }

  void setReduceInputRecords(long reduceInputRecords) {
    this.reduceInputRecords = reduceInputRecords;
  }

  public long getReduceShuffleBytes() {
    return reduceShuffleBytes;
  }

  void setReduceShuffleBytes(long reduceShuffleBytes) {
    this.reduceShuffleBytes = reduceShuffleBytes;
  }

  public long getReduceOutputRecords() {
    return reduceOutputRecords;
  }

  void setReduceOutputRecords(long reduceOutputRecords) {
    this.reduceOutputRecords = reduceOutputRecords;
  }

  public long getSpilledRecords() {
    return spilledRecords;
  }

  void setSpilledRecords(long spilledRecords) {
    this.spilledRecords = spilledRecords;
  }

  public LoggedLocation getLocation() {
    return location;
  }

  void setLocation(LoggedLocation location) {
    this.location = location;
  }

  public long getMapInputBytes() {
    return mapInputBytes;
  }

  void setMapInputBytes(long mapInputBytes) {
    this.mapInputBytes = mapInputBytes;
  }

  // incorporate event counters
  public void incorporateCounters(JhCounters counters) {
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.hdfsBytesRead = val;
      }
    }, counters, "HDFS_BYTES_READ");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.hdfsBytesWritten = val;
      }
    }, counters, "HDFS_BYTES_WRITTEN");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.fileBytesRead = val;
      }
    }, counters, "FILE_BYTES_READ");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.fileBytesWritten = val;
      }
    }, counters, "FILE_BYTES_WRITTEN");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.mapInputBytes = val;
      }
    }, counters, "MAP_INPUT_BYTES");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.mapInputRecords = val;
      }
    }, counters, "MAP_INPUT_RECORDS");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.mapOutputBytes = val;
      }
    }, counters, "MAP_OUTPUT_BYTES");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.mapOutputRecords = val;
      }
    }, counters, "MAP_OUTPUT_RECORDS");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.combineInputRecords = val;
      }
    }, counters, "COMBINE_INPUT_RECORDS");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.reduceInputGroups = val;
      }
    }, counters, "REDUCE_INPUT_GROUPS");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.reduceInputRecords = val;
      }
    }, counters, "REDUCE_INPUT_RECORDS");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.reduceShuffleBytes = val;
      }
    }, counters, "REDUCE_SHUFFLE_BYTES");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.reduceOutputRecords = val;
      }
    }, counters, "REDUCE_OUTPUT_RECORDS");
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        attempt.spilledRecords = val;
      }
    }, counters, "SPILLED_RECORDS");
    
    // incorporate CPU usage
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        metrics.setCumulativeCpuUsage(val);
      }
    }, counters, "CPU_MILLISECONDS");
    
    // incorporate virtual memory usage
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        metrics.setVirtualMemoryUsage(val);
      }
    }, counters, "VIRTUAL_MEMORY_BYTES");
    
    // incorporate physical memory usage
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        metrics.setPhysicalMemoryUsage(val);
      }
    }, counters, "PHYSICAL_MEMORY_BYTES");
    
    // incorporate heap usage
    incorporateCounter(new SetField(this) {
      @Override
      void set(long val) {
        metrics.setHeapUsage(val);
      }
    }, counters, "COMMITTED_HEAP_BYTES");
  }

  // Get the resource usage metrics
  public ResourceUsageMetrics getResourceUsageMetrics() {
    return metrics;
  }
  
  // Set the resource usage metrics
  void setResourceUsageMetrics(ResourceUsageMetrics metrics) {
    this.metrics = metrics;
  }
  
  private static String canonicalizeCounterName(String nonCanonicalName) {
    String result = StringUtils.toLowerCase(nonCanonicalName);

    result = result.replace(' ', '|');
    result = result.replace('-', '|');
    result = result.replace('_', '|');
    result = result.replace('.', '|');

    return result;
  }

  private abstract class SetField {
    LoggedTaskAttempt attempt;

    SetField(LoggedTaskAttempt attempt) {
      this.attempt = attempt;
    }

    abstract void set(long value);
  }

  private static void incorporateCounter(SetField thunk, JhCounters counters,
      String counterName) {
    counterName = canonicalizeCounterName(counterName);

    for (JhCounterGroup group : counters.groups) {
      for (JhCounter counter : group.counts) {
        if (counterName
            .equals(canonicalizeCounterName(counter.name.toString()))) {
          thunk.set(counter.value);
          return;
        }
      }
    }
  }

  private void compare1(String c1, String c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null || !c1.equals(c2)) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(NodeName c1, NodeName c2, TreePath loc, String eltname)
  throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }

    compare1(c1.getValue(), c2.getValue(), new TreePath(loc, eltname), "value");
  }

  private void compare1(long c1, long c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(Pre21JobHistoryConstants.Values c1,
      Pre21JobHistoryConstants.Values c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(LoggedLocation c1, LoggedLocation c2, TreePath loc,
      String eltname) throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    TreePath recurse = new TreePath(loc, eltname);

    if (c1 == null || c2 == null) {
      throw new DeepInequalityException(eltname + " miscompared", recurse);
    }

    c1.deepCompare(c2, recurse);
  }

  private void compare1(List<Integer> c1, List<Integer> c2, TreePath loc,
                        String eltname)
        throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null || c1.size() != c2.size()) {
      throw new DeepInequalityException
              (eltname + " miscompared", new TreePath(loc, eltname));
    }

    for (int i = 0; i < c1.size(); ++i) {
      if (!c1.get(i).equals(c2.get(i))) {
        throw new DeepInequalityException("" + c1.get(i) + " != " + c2.get(i),
                                          new TreePath(loc, eltname, i));
      }
    }
  }    

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedTaskAttempt)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedTaskAttempt other = (LoggedTaskAttempt) comparand;

    compare1(attemptID.toString(), other.attemptID.toString(), loc, "attemptID");
    compare1(result, other.result, loc, "result");
    compare1(startTime, other.startTime, loc, "startTime");
    compare1(finishTime, other.finishTime, loc, "finishTime");
    compare1(hostName, other.hostName, loc, "hostName");

    compare1(hdfsBytesRead, other.hdfsBytesRead, loc, "hdfsBytesRead");
    compare1(hdfsBytesWritten, other.hdfsBytesWritten, loc, "hdfsBytesWritten");
    compare1(fileBytesRead, other.fileBytesRead, loc, "fileBytesRead");
    compare1(fileBytesWritten, other.fileBytesWritten, loc, "fileBytesWritten");
    compare1(mapInputBytes, other.mapInputBytes, loc, "mapInputBytes");
    compare1(mapInputRecords, other.mapInputRecords, loc, "mapInputRecords");
    compare1(mapOutputBytes, other.mapOutputBytes, loc, "mapOutputBytes");
    compare1(mapOutputRecords, other.mapOutputRecords, loc, "mapOutputRecords");
    compare1(combineInputRecords, other.combineInputRecords, loc,
        "combineInputRecords");
    compare1(reduceInputGroups, other.reduceInputGroups, loc,
        "reduceInputGroups");
    compare1(reduceInputRecords, other.reduceInputRecords, loc,
        "reduceInputRecords");
    compare1(reduceShuffleBytes, other.reduceShuffleBytes, loc,
        "reduceShuffleBytes");
    compare1(reduceOutputRecords, other.reduceOutputRecords, loc,
        "reduceOutputRecords");
    compare1(spilledRecords, other.spilledRecords, loc, "spilledRecords");

    compare1(shuffleFinished, other.shuffleFinished, loc, "shuffleFinished");
    compare1(sortFinished, other.sortFinished, loc, "sortFinished");

    compare1(location, other.location, loc, "location");

    compare1(clockSplits, other.clockSplits, loc, "clockSplits");
    compare1(cpuUsages, other.cpuUsages, loc, "cpuUsages");
    compare1(vMemKbytes, other.vMemKbytes, loc, "vMemKbytes");
    compare1(physMemKbytes, other.physMemKbytes, loc, "physMemKbytes");
  }
}