/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


import java.io.IOException;
import java.io.File;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.UTF8;

import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.HashPartitioner;

/** A map/reduce job configuration.  This names the {@link Mapper}, combiner
 * (if any), {@link Partitioner}, {@link Reducer}, {@link InputFormat}, and
 * {@link OutputFormat} implementations to be used.  It also indicates the set
 * of input files, and where the output files should be written. */
public class JobConf extends Configuration {

  public JobConf() {
    super();
  }
    
  /**
   * Construct a map/reduce job configuration.
   * 
   * @param conf
   *          a Configuration whose settings will be inherited.
   */
  public JobConf(Configuration conf) {
    super(conf);
    addDefaultResource("mapred-default.xml");
  }


  /** Construct a map/reduce configuration.
   *
   * @param config a Configuration-format XML job description file
   */
  public JobConf(String config) {
    this(new File(config));
  }

  /** Construct a map/reduce configuration.
   *
   * @param config a Configuration-format XML job description file
   */
  public JobConf(File config) {
    super();
    addDefaultResource("mapred-default.xml");
    addDefaultResource(config);
  }

  public String getJar() { return get("mapred.jar"); }
  public void setJar(String jar) { set("mapred.jar", jar); }

  public File getSystemDir() {
    return new File(get("mapred.system.dir",
                                        "/tmp/hadoop/mapred/system"));
  }

  public String[] getLocalDirs() throws IOException {
    return getStrings("mapred.local.dir");
  }

  public void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileUtil.fullyDelete(new File(localDirs[i]), this);
    }
  }

  public void deleteLocalFiles(String subdir) throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileUtil.fullyDelete(new File(localDirs[i], subdir), this);
    }
  }

  /** Constructs a local file name.  Files are distributed among configured
   * local directories.*/
  public File getLocalFile(String subdir, String name) throws IOException {
      String param[] = new String[1];
      param[0] = name;
      return getLocalFile(subdir, param, "", false);
  }
  // REMIND - mjc - rename this!  getLocalFile() is not quite the same.
  public File getLocalFile(String subdir, String names[], String ending) throws IOException {
      return getLocalFile(subdir, names, ending, true);
  }
  File getLocalFile(String subdir, String names[], String ending, boolean existingFileTest) throws IOException {
    String[] localDirs = getLocalDirs();
    for (int k = 0; k < names.length; k++) {
        String path = subdir + File.separator + names[k] + ending;
        int hashCode = path.hashCode();
        for (int i = 0; i < localDirs.length; i++) {  // try each local dir
            int index = (hashCode+i & Integer.MAX_VALUE) % localDirs.length;
            File file = new File(localDirs[index], path);
            File dir = file.getParentFile();
            if (existingFileTest) {
                if (file.exists()) {
                    return file;
                }
            } else {
                if (dir.exists() || dir.mkdirs()) {
                    return file;
                }
            }
        }
    }
    throw new IOException("No valid local directories.");
  }

  public void setInputDir(File dir) { set("mapred.input.dir", dir); }

  public void addInputDir(File dir) {
    String dirs = get("mapred.input.dir");
    set("mapred.input.dir", dirs == null ? dir.toString() : dirs + "," + dir);
  }
  public File[] getInputDirs() {
    String dirs = get("mapred.input.dir", "");
    ArrayList list = Collections.list(new StringTokenizer(dirs, ","));
    File[] result = new File[list.size()];
    for (int i = 0; i < list.size(); i++) {
      result[i] = new File((String)list.get(i));
    }
    return result;
  }

  public File getOutputDir() { return new File(get("mapred.output.dir")); }
  public void setOutputDir(File dir) { set("mapred.output.dir", dir); }

  public InputFormat getInputFormat() {
    return (InputFormat)newInstance(getClass("mapred.input.format.class",
                                             TextInputFormat.class,
                                             InputFormat.class));
  }
  public void setInputFormat(Class theClass) {
    setClass("mapred.input.format.class", theClass, InputFormat.class);
  }
  public OutputFormat getOutputFormat() {
    return (OutputFormat)newInstance(getClass("mapred.output.format.class",
                                              TextOutputFormat.class,
                                              OutputFormat.class));
  }
  public void setOutputFormat(Class theClass) {
    setClass("mapred.output.format.class", theClass, OutputFormat.class);
  }
  
  public Class getInputKeyClass() {
    return getClass("mapred.input.key.class",
                    LongWritable.class, WritableComparable.class);
  }
  public void setInputKeyClass(Class theClass) {
    setClass("mapred.input.key.class", theClass, WritableComparable.class);
  }

  public Class getInputValueClass() {
    return getClass("mapred.input.value.class", UTF8.class, Writable.class);
  }
  public void setInputValueClass(Class theClass) {
    setClass("mapred.input.value.class", theClass, Writable.class);
  }

  public Class getOutputKeyClass() {
    return getClass("mapred.output.key.class",
                    LongWritable.class, WritableComparable.class);
  }
  public void setOutputKeyClass(Class theClass) {
    setClass("mapred.output.key.class", theClass, WritableComparable.class);
  }

  public WritableComparator getOutputKeyComparator() {
    Class theClass = getClass("mapred.output.key.comparator.class", null,
                              WritableComparator.class);
    if (theClass != null)
      return (WritableComparator)newInstance(theClass);
    return WritableComparator.get(getOutputKeyClass());
  }

  public void setOutputKeyComparatorClass(Class theClass) {
    setClass("mapred.output.key.comparator.class",
             theClass, WritableComparator.class);
  }

  public Class getOutputValueClass() {
    return getClass("mapred.output.value.class", UTF8.class, Writable.class);
  }
  public void setOutputValueClass(Class theClass) {
    setClass("mapred.output.value.class", theClass, Writable.class);
  }

  public Class getMapperClass() {
    return getClass("mapred.mapper.class", IdentityMapper.class, Mapper.class);
  }
  public void setMapperClass(Class theClass) {
    setClass("mapred.mapper.class", theClass, Mapper.class);
  }

  public Class getMapRunnerClass() {
    return getClass("mapred.map.runner.class",
                    MapRunner.class, MapRunnable.class);
  }
  public void setMapRunnerClass(Class theClass) {
    setClass("mapred.map.runner.class", theClass, MapRunnable.class);
  }

  public Class getPartitionerClass() {
    return getClass("mapred.partitioner.class",
                    HashPartitioner.class, Partitioner.class);
  }
  public void setPartitionerClass(Class theClass) {
    setClass("mapred.partitioner.class", theClass, Partitioner.class);
  }

  public Class getReducerClass() {
    return getClass("mapred.reducer.class",
                    IdentityReducer.class, Reducer.class);
  }
  public void setReducerClass(Class theClass) {
    setClass("mapred.reducer.class", theClass, Reducer.class);
  }

  public Class getCombinerClass() {
    return getClass("mapred.combiner.class", null, Reducer.class);
  }
  public void setCombinerClass(Class theClass) {
    setClass("mapred.combiner.class", theClass, Reducer.class);
  }
  
  public int getNumMapTasks() { return getInt("mapred.map.tasks", 1); }
  public void setNumMapTasks(int n) { setInt("mapred.map.tasks", n); }

  public int getNumReduceTasks() { return getInt("mapred.reduce.tasks", 1); }
  public void setNumReduceTasks(int n) { setInt("mapred.reduce.tasks", n); }

  public Object newInstance(Class theClass) {
    Object result;
    try {
      result = theClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (result instanceof JobConfigurable)
      ((JobConfigurable)result).configure(this);
    return result;
  }

}

