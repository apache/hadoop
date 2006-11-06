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


import java.io.IOException;
import java.io.File;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

import java.net.URL;
import java.net.URLDecoder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;

/** A map/reduce job configuration.  This names the {@link Mapper}, combiner
 * (if any), {@link Partitioner}, {@link Reducer}, {@link InputFormat}, and
 * {@link OutputFormat} implementations to be used.  It also indicates the set
 * of input files, and where the output files should be written. */
public class JobConf extends Configuration {

  private void initialize() {
    addDefaultResource("mapred-default.xml");
  }
  
  private void initialize(Class exampleClass) {
    initialize();
    String jar = findContainingJar(exampleClass);
    if (jar != null) {
      setJar(jar);
    }   
  }
  
  /**
   * Construct a map/reduce job configuration.
   */
  public JobConf() {
    initialize();
  }

  /** 
   * Construct a map/reduce job configuration.
   * @param exampleClass a class whose containing jar is used as the job's jar.
   */
  public JobConf(Class exampleClass) {
    initialize(exampleClass);
  }
  
  /**
   * Construct a map/reduce job configuration.
   * 
   * @param conf a Configuration whose settings will be inherited.
   */
  public JobConf(Configuration conf) {
    super(conf);
    initialize();
  }


  /** Construct a map/reduce job configuration.
   * 
   * @param conf a Configuration whose settings will be inherited.
   * @param exampleClass a class whose containing jar is used as the job's jar.
   */
  public JobConf(Configuration conf, Class exampleClass) {
    this(conf);
    initialize(exampleClass);
  }


  /** Construct a map/reduce configuration.
   *
   * @param config a Configuration-format XML job description file
   */
  public JobConf(String config) {
    this(new Path(config));
  }

  /** Construct a map/reduce configuration.
   *
   * @param config a Configuration-format XML job description file
   */
  public JobConf(Path config) {
    super();
    addDefaultResource("mapred-default.xml");
    addDefaultResource(config);
  }

  public String getJar() { return get("mapred.jar"); }
  public void setJar(String jar) { set("mapred.jar", jar); }

  public Path getSystemDir() {
    return new Path(get("mapred.system.dir", "/tmp/hadoop/mapred/system"));
  }

  public String[] getLocalDirs() throws IOException {
    return getStrings("mapred.local.dir");
  }

  public void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getNamed("local", this).delete(new Path(localDirs[i]));
    }
  }

  public void deleteLocalFiles(String subdir) throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getNamed("local", this).delete(new Path(localDirs[i], subdir));
    }
  }

  /** Constructs a local file name.  Files are distributed among configured
   * local directories.*/
  public Path getLocalPath(String pathString) throws IOException {
    return getLocalPath("mapred.local.dir", pathString);
  }

  public void setInputPath(Path dir) {
    dir = new Path(getWorkingDirectory(), dir);
    set("mapred.input.dir", dir);
  }

  public void addInputPath(Path dir) {
    dir = new Path(getWorkingDirectory(), dir);
    String dirs = get("mapred.input.dir");
    set("mapred.input.dir", dirs == null ? dir.toString() : dirs + "," + dir);
  }

  public Path[] getInputPaths() {
    String dirs = get("mapred.input.dir", "");
    ArrayList list = Collections.list(new StringTokenizer(dirs, ","));
    Path[] result = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      result[i] = new Path((String)list.get(i));
    }
    return result;
  }

  /**
   * Get the reported username for this job.
   * @return the username
   */
  public String getUser() {
    return get("user.name");
  }
  
  /**
   * Set the reported username for this job.
   * @param user the username
   */
  public void setUser(String user) {
    set("user.name", user);
  }


  
  /**
   * Set whether the framework should keep the intermediate files for 
   * failed tasks.
   */
  public void setKeepFailedTaskFiles(boolean keep) {
    setBoolean("keep.failed.task.files", keep);
  }
  
  /**
   * Should the temporary files for failed tasks be kept?
   * @return should the files be kept?
   */
  public boolean getKeepFailedTaskFiles() {
    return getBoolean("keep.failed.task.files", false);
  }
  
  /**
   * Set a regular expression for task names that should be kept. 
   * The regular expression ".*_m_000123_0" would keep the files
   * for the first instance of map 123 that ran.
   * @param pattern the java.util.regex.Pattern to match against the 
   *        task names.
   */
  public void setKeepTaskFilesPattern(String pattern) {
    set("keep.task.files.pattern", pattern);
  }
  
  /**
   * Get the regular expression that is matched against the task names
   * to see if we need to keep the files.
   * @return the pattern as a string, if it was set, othewise null
   */
  public String getKeepTaskFilesPattern() {
    return get("keep.task.files.pattern");
  }
  
  /**
   * Set the current working directory for the default file system
   * @param dir the new current working directory
   */
  public void setWorkingDirectory(Path dir) {
    dir = new Path(getWorkingDirectory(), dir);
    set("mapred.working.dir", dir.toString());
  }
  
  /**
   * Get the current working directory for the default file system.
   * @return the directory name
   */
  public Path getWorkingDirectory() {
    String name = get("mapred.working.dir");
    if (name != null) {
      return new Path(name);
    } else {
      try {
        Path dir = FileSystem.get(this).getWorkingDirectory();
        set("mapred.working.dir", dir.toString());
        return dir;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  public Path getOutputPath() { 
    String name = get("mapred.output.dir");
    return name == null ? null: new Path(name);
  }

  public void setOutputPath(Path dir) {
    dir = new Path(getWorkingDirectory(), dir);
    set("mapred.output.dir", dir);
  }

  public InputFormat getInputFormat() {
    return (InputFormat)ReflectionUtils.newInstance(getClass("mapred.input.format.class",
                                             TextInputFormat.class,
                                             InputFormat.class),
                                             this);
  }
  public void setInputFormat(Class theClass) {
    setClass("mapred.input.format.class", theClass, InputFormat.class);
  }
  public OutputFormat getOutputFormat() {
    return (OutputFormat)ReflectionUtils.newInstance(getClass("mapred.output.format.class",
                                              TextOutputFormat.class,
                                              OutputFormat.class),
                                              this);
  }
  public void setOutputFormat(Class theClass) {
    setClass("mapred.output.format.class", theClass, OutputFormat.class);
  }

  /** @deprecated Call {@link RecordReader#createKey()}. */
  public Class getInputKeyClass() {
    return getClass("mapred.input.key.class",
                    LongWritable.class, WritableComparable.class);
  }

  /** @deprecated Not used */
  public void setInputKeyClass(Class theClass) {
    setClass("mapred.input.key.class", theClass, WritableComparable.class);
  }

  /** @deprecated Call {@link RecordReader#createValue()}. */
  public Class getInputValueClass() {
    return getClass("mapred.input.value.class", Text.class, Writable.class);
  }

  /** @deprecated Not used */
  public void setInputValueClass(Class theClass) {
    setClass("mapred.input.value.class", theClass, Writable.class);
  }
  
  /**
   * Should the map outputs be compressed before transfer?
   * Uses the SequenceFile compression.
   */
  public void setCompressMapOutput(boolean compress) {
    setBoolean("mapred.compress.map.output", compress);
  }
  
  /**
   * Are the outputs of the maps be compressed?
   * @return are they compressed?
   */
  public boolean getCompressMapOutput() {
    return getBoolean("mapred.compress.map.output", false);
  }

  /**
   * Set the compression type for the map outputs.
   * @param style NONE, RECORD, or BLOCK to control how the map outputs are 
   *        compressed
   */
  public void setMapOutputCompressionType(SequenceFile.CompressionType style) {
    set("map.output.compression.type", style.toString());
  }
  
  /**
   * Get the compression type for the map outputs.
   * @return the compression type, defaulting to job output compression type
   */
  public SequenceFile.CompressionType getMapOutputCompressionType() {
    String val = get("map.output.compression.type");
    if (val == null) {
      return SequenceFile.getCompressionType(this);
    } else {
      return SequenceFile.CompressionType.valueOf(val);
    }
  }
  
  /**
   * Set the given class as the  compression codec for the map outputs.
   * @param codecClass the CompressionCodec class that will compress the 
   *                   map outputs
   */
  public void setMapOutputCompressorClass(Class codecClass) {
    setCompressMapOutput(true);
    setClass("mapred.output.compression.codec", codecClass, 
             CompressionCodec.class);
  }
  
  /**
   * Get the codec for compressing the map outputs
   * @param defaultValue the value to return if it is not set
   * @return the CompressionCodec class that should be used to compress the 
   *   map outputs
   * @throws IllegalArgumentException if the class was specified, but not found
   */
  public Class getMapOutputCompressorClass(Class defaultValue) {
    String name = get("mapred.output.compression.codec");
    if (name == null) {
      return defaultValue;
    } else {
      try {
        return getClassByName(name);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name + 
                                           " was not found.", e);
      }
    }
  }
  
  /**
   * Get the key class for the map output data. If it is not set, use the
   * (final) output ket class This allows the map output key class to be
   * different than the final output key class
   * 
   * @return map output key class
   */
  public Class getMapOutputKeyClass() {
    Class retv = getClass("mapred.mapoutput.key.class", null,
			  WritableComparable.class);
    if (retv == null) {
      retv = getOutputKeyClass();
    }
    return retv;
  }
  
  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * value class
   */
  public void setMapOutputKeyClass(Class theClass) {
    setClass("mapred.mapoutput.key.class", theClass,
             WritableComparable.class);
  }
  
  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class
   * 
   * @return map output value class
   */
  public Class getMapOutputValueClass() {
    Class retv = getClass("mapred.mapoutput.value.class", null,
			  Writable.class);
    if (retv == null) {
      retv = getOutputValueClass();
    }
    return retv;
  }
  
  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class
   */
  public void setMapOutputValueClass(Class theClass) {
    setClass("mapred.mapoutput.value.class", theClass, Writable.class);
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
      return (WritableComparator)ReflectionUtils.newInstance(theClass, this);
    return WritableComparator.get(getMapOutputKeyClass());
  }

  public void setOutputKeyComparatorClass(Class theClass) {
    setClass("mapred.output.key.comparator.class",
             theClass, WritableComparator.class);
  }

  public Class getOutputValueClass() {
    return getClass("mapred.output.value.class", Text.class, Writable.class);
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
  
  /**
   * Should speculative execution be used for this job?
   * @return Defaults to true
   */
  public boolean getSpeculativeExecution() { 
    return getBoolean("mapred.speculative.execution", true);
  }
  
  /**
   * Turn on or off speculative execution for this job.
   * In general, it should be turned off for map jobs that have side effects.
   */
  public void setSpeculativeExecution(boolean new_val) {
    setBoolean("mapred.speculative.execution", new_val);
  }
  
  public int getNumMapTasks() { return getInt("mapred.map.tasks", 1); }
  public void setNumMapTasks(int n) { setInt("mapred.map.tasks", n); }

  public int getNumReduceTasks() { return getInt("mapred.reduce.tasks", 1); }
  public void setNumReduceTasks(int n) { setInt("mapred.reduce.tasks", n); }

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * @return the job's name, defaulting to ""
   */
  public String getJobName() {
    return get("mapred.job.name", "");
  }
  
  /**
   * Set the user-specified job name.
   * @param name the job's new name
   */
  public void setJobName(String name) {
    set("mapred.job.name", name);
  }
  
  /** Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * @author Owen O'Malley
   * @param my_class the class to find
   * @return a jar file that contains the class, or null
   * @throws IOException
   */
  private static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

}

