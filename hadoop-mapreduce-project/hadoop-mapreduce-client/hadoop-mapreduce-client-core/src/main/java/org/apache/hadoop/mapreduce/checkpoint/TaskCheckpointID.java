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
package org.apache.hadoop.mapreduce.checkpoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;

/**
 * Implementation of CheckpointID used in MR. It contains a reference to an
 * underlying FileSsytem based checkpoint, and various metadata about the
 * cost of checkpoints and other counters. This is sent by the task to the AM
 * to be stored and provided to the next execution of the same task.
 */
public class TaskCheckpointID implements CheckpointID {

  final FSCheckpointID rawId;
  private final List<Path> partialOutput;
  private final Counters counters;

  public TaskCheckpointID() {
    this(new FSCheckpointID(), new ArrayList<Path>(), new Counters());
  }

  public TaskCheckpointID(FSCheckpointID rawId, List<Path> partialOutput,
          Counters counters) {
    this.rawId = rawId;
    this.counters = counters;
    this.partialOutput = null == partialOutput
      ? new ArrayList<Path>()
      : partialOutput;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    counters.write(out);
    WritableUtils.writeVLong(out, partialOutput.size());
    for (Path p : partialOutput) {
      Text.writeString(out, p.toString());
    }
    rawId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    partialOutput.clear();
    counters.readFields(in);
    long numPout = WritableUtils.readVLong(in);
    for (int i = 0; i < numPout; i++) {
      partialOutput.add(new Path(Text.readString(in)));
    }
    rawId.readFields(in);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TaskCheckpointID){
      TaskCheckpointID o = (TaskCheckpointID) other;
      return rawId.equals(o.rawId) &&
             counters.equals(o.counters) &&
             partialOutput.containsAll(o.partialOutput) &&
             o.partialOutput.containsAll(partialOutput);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return rawId.hashCode();
  }

  /**
   * @return the size of the checkpoint in bytes
   */
  public long getCheckpointBytes() {
    return counters.findCounter(EnumCounter.CHECKPOINT_BYTES).getValue();
  }

  /**
   * @return how long it took to take this checkpoint
   */
  public long getCheckpointTime() {
    return counters.findCounter(EnumCounter.CHECKPOINT_MS).getValue();
  }

  public String toString() {
    return rawId.toString() + " counters:" + counters;

  }

  public List<Path> getPartialCommittedOutput() {
    return partialOutput;
  }

  public Counters getCounters() {
    return counters;
  }

}
