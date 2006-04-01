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
import java.util.Enumeration;

import java.net.URL;
import java.net.URLDecoder;

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


  /** Construct a map/reduce job configuration.
   * 
   * @param conf a Configuration whose settings will be inherited.
   * @param aClass a class whose containing jar is used as the job's jar.
   */
  public JobConf(Configuration conf, Class aClass) {
    this(conf);
    String jar = findContainingJar(aClass);
    if (jar != null) {
      setJar(jar);
    }
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
    return new File(get("mapred.system.dir", "/tmp/hadoop/mapred/system"))
      .getAbsoluteFile();
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
    return getFile("mapred.local.dir", name + File.separator + subdir);
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
   * Set the current working directory for the default file system
   * @param dir the new current working directory
   */
  public void setWorkingDirectory(String dir) {
    set("mapred.working.dir", dir);
  }
  
  /**
   * Get the current working directory for the default file system.
   * @return the directory name
   */
  public String getWorkingDirectory() {
    return get("mapred.working.dir"); 
  }
  
  public File getOutputDir() { 
    String name = get("mapred.output.dir");
    return name == null ? null: new File(name);
  }

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

