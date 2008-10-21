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

package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HiveInputFormat is a parameterized InputFormat which looks at the path name and determine
 * the correct InputFormat for that path name from mapredPlan.pathToPartitionInfo().
 * It can be used to read files with different input format in the same map-reduce job.
 */
public class HiveInputFormat<K extends WritableComparable,
                             V extends Writable> implements InputFormat<K, V>, JobConfigurable {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.hive.ql.io.HiveInputFormat");

  /**
   * HiveInputSplit encapsulates an InputSplit with its corresponding inputFormatClass.
   * The reason that it derives from FileSplit is to make sure "map.input.file" in MapTask.
   */
  public static class HiveInputSplit extends FileSplit implements InputSplit {

    InputSplit inputSplit;
    String     inputFormatClassName;

    public HiveInputSplit() {
      // This is the only public constructor of FileSplit
      super((Path)null, 0, 0, (String[])null);
    }

    public HiveInputSplit(InputSplit inputSplit, String inputFormatClassName) {
      // This is the only public constructor of FileSplit
      super((Path)null, 0, 0, (String[])null);
      this.inputSplit = inputSplit;
      this.inputFormatClassName = inputFormatClassName;
    }

    public InputSplit getInputSplit() {
      return inputSplit;
    }
    public String inputFormatClassName() {
      return inputFormatClassName;
    }

    public Path getPath() {
      if (inputSplit instanceof FileSplit) {
        return ((FileSplit)inputSplit).getPath();
      }
      return new Path("");
    }

    /** The position of the first byte in the file to process. */
    public long getStart() {
      if (inputSplit instanceof FileSplit) {
        return ((FileSplit)inputSplit).getStart();
      }
      return 0;
    }

    public String toString() {
      return inputFormatClassName + ":" + inputSplit.toString();
    }

    public long getLength() {
      long r = 0;
      try {
        r = inputSplit.getLength();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return r;
    }

    public String[] getLocations() throws IOException {
      return inputSplit.getLocations();
    }

    public void readFields(DataInput in) throws IOException {
      String inputSplitClassName = in.readUTF();
      try {
        inputSplit = (InputSplit) ReflectionUtils.newInstance(Class.forName(inputSplitClassName), job);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputSplit class = "
            + inputSplitClassName + ":" + e.getMessage());
      }
      inputSplit.readFields(in);
      inputFormatClassName = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(inputSplit.getClass().getName());
      inputSplit.write(out);
      out.writeUTF(inputFormatClassName);
    }
  }

  static JobConf job;

  public void configure(JobConf job) {
    HiveInputFormat.job = job;
  }

  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class,InputFormat<WritableComparable, Writable>> inputFormats;
  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(Class inputFormatClass) throws IOException {
    if (inputFormats == null) {
      inputFormats = new HashMap<Class, InputFormat<WritableComparable, Writable>>();
    }
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance =
            (InputFormat<WritableComparable, Writable>)ReflectionUtils.newInstance(inputFormatClass, job);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class " + inputFormatClass.getName()
            + " as specified in mapredWork!");
      }
    }
    return inputFormats.get(inputFormatClass);
  }

  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {

    HiveInputSplit hsplit = (HiveInputSplit)split;

    InputSplit inputSplit = hsplit.getInputSplit();
    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = Class.forName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass);
    
    return new HiveRecordReader(inputFormat.getRecordReader(inputSplit, job, reporter));
  }


  private Map<String, partitionDesc> pathToPartitionInfo;

  protected void init(JobConf job) {
    mapredWork mrwork = Utilities.getMapRedWork(job);
    pathToPartitionInfo = mrwork.getPathToPartitionInfo();
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // for each dir, get the InputFormat, and do getSplits.
    for(Path dir: dirs) {
      tableDesc table = getTableDescFromPath(dir);
      // create a new InputFormat instance if this is the first time to see this class
      Class inputFormatClass = table.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass);

      FileInputFormat.setInputPaths(newjob, dir);
      newjob.setInputFormat(inputFormat.getClass());
      InputSplit[] iss = inputFormat.getSplits(newjob, numSplits/dirs.length);
      for(InputSplit is: iss) {
        result.add(new HiveInputSplit(is, inputFormatClass.getName()));
      }
    }
    return result.toArray(new HiveInputSplit[result.size()]);
  }


  private tableDesc getTableDescFromPath(Path dir) throws IOException {

    partitionDesc partDesc = pathToPartitionInfo.get(dir.toString());
    if (partDesc == null) {
      partDesc = pathToPartitionInfo.get(dir.toUri().getPath());
    }
    if (partDesc == null) {
      throw new IOException("cannot find dir = " + dir.toString() + " in partToPartitionInfo!");
    }

    tableDesc table = partDesc.getTableDesc();
    if (table == null) {
      throw new IOException("Input " + dir.toString() +
          " does not have associated InputFormat in mapredWork!");
    }

    return table;
  }


}
