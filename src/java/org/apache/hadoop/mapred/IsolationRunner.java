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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;

public class IsolationRunner {
  private static final Log LOG = 
    LogFactory.getLog(IsolationRunner.class.getName());

  private static class FakeUmbilical implements TaskUmbilicalProtocol {

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public void done(String taskid) throws IOException {
      LOG.info("Task " + taskid + " reporting done.");
    }

    public void fsError(String message) throws IOException {
      LOG.info("Task reporting file system error: " + message);
    }

    public Task getTask(String taskid) throws IOException {
      return null;
    }

    public boolean ping(String taskid) throws IOException {
      return true;
    }

    public void progress(String taskid, float progress, String state,
                         TaskStatus.Phase phase) throws IOException {
      StringBuffer buf = new StringBuffer("Task ");
      buf.append(taskid);
      buf.append(" making progress to ");
      buf.append(progress);
      if (state != null) {
        buf.append(" and state of ");
        buf.append(state);
      }
      LOG.info(buf.toString());
      // ignore phase
    }

    public void reportDiagnosticInfo(String taskid, String trace) throws IOException {
      LOG.info("Task " + taskid + " has problem " + trace);
    }
    
  }
  
  private static ClassLoader makeClassLoader(JobConf conf, 
                                             File workDir) throws IOException {
    List cp = new ArrayList();

    String jar = conf.getJar();
    if (jar != null) {                      // if jar exists, it into workDir
      File[] libs = new File(workDir, "lib").listFiles();
      if (libs != null) {
        for (int i = 0; i < libs.length; i++) {
          cp.add(new URL("file:" + libs[i].toString()));
        }
      }
      cp.add(new URL("file:" + new File(workDir, "classes/").toString()));
      cp.add(new URL("file:" + workDir.toString() + "/"));
    }
    return new URLClassLoader((URL[]) cp.toArray(new URL[cp.size()]));
  }
  
  /**
   * Create empty sequence files for any of the map outputs that we don't have.
   * @param fs the filesystem to create the files in
   * @param dir the directory name to create the files in
   * @param conf the jobconf
   * @throws IOException if something goes wrong writing
   */
  private static void fillInMissingMapOutputs(FileSystem fs, 
                                              String taskId,
                                              int numMaps,
                                              JobConf conf) throws IOException {
    Class keyClass = conf.getMapOutputKeyClass();
    Class valueClass = conf.getMapOutputValueClass();
    MapOutputFile namer = new MapOutputFile();
    namer.setConf(conf);
    for(int i=0; i<numMaps; i++) {
      Path f = namer.getInputFile(i, taskId);
      if(! fs.exists(f)) {
        LOG.info("Create missing input: " + f);
        SequenceFile.Writer out =
          SequenceFile.createWriter(fs, conf, f, keyClass, valueClass);
        out.close();
      }
    }    
  }
  
  /**
   * Run a single task
   * @param args the first argument is the task directory
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.out.println("Usage: IsolationRunner <path>/job.xml");
      System.exit(1);
    }
    File jobFilename = new File(args[0]);
    if (!jobFilename.exists() || !jobFilename.isFile()) {
      System.out.println(jobFilename + " is not a valid job file.");
      System.exit(1);
    }
    JobConf conf = new JobConf(new Path(jobFilename.toString()));
    String taskId = conf.get("mapred.task.id");
    boolean isMap = conf.getBoolean("mapred.task.is.map", true);
    String jobId = conf.get("mapred.job.id");
    int partition = conf.getInt("mapred.task.partition", 0);
    
    // setup the local and user working directories
    FileSystem local = FileSystem.getNamed("local", conf);
    File workDirName = new File(jobFilename.getParent(), "work");
    local.setWorkingDirectory(new Path(workDirName.toString()));
    FileSystem.get(conf).setWorkingDirectory(conf.getWorkingDirectory());
    
    // set up a classloader with the right classpath
    ClassLoader classLoader = makeClassLoader(conf, workDirName);
    Thread.currentThread().setContextClassLoader(classLoader);
    conf.setClassLoader(classLoader);
    
    Task task;
    if (isMap) {
      FileSplit split = new FileSplit(new Path(conf.get("map.input.file")),
                                      conf.getLong("map.input.start", 0),
                                      conf.getLong("map.input.length", 0));
      task = new MapTask(jobId, jobFilename.toString(), conf.get("mapred.tip.id"), 
          taskId, partition, split);
    } else {
      int numMaps = conf.getNumMapTasks();
      fillInMissingMapOutputs(local, taskId, numMaps, conf);
      task = new ReduceTask(jobId, jobFilename.toString(), conf.get("mapred.tip.id"), taskId, 
                            partition, numMaps);
    }
    task.setConf(conf);
    task.run(conf, new FakeUmbilical());
  }

}
