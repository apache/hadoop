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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JvmTask;

public class IsolationRunner {
  private static final Log LOG = 
    LogFactory.getLog(IsolationRunner.class.getName());

  private static class FakeUmbilical implements TaskUmbilicalProtocol {

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public void done(TaskAttemptID taskid) throws IOException {
      LOG.info("Task " + taskid + " reporting done.");
    }

    public void fsError(TaskAttemptID taskId, String message) throws IOException {
      LOG.info("Task " + taskId + " reporting file system error: " + message);
    }

    public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
      LOG.info("Task " + taskId + " reporting shuffle error: " + message);
    }

    public void fatalError(TaskAttemptID taskId, String msg) throws IOException{
      LOG.info("Task " + taskId + " reporting fatal error: " + msg);
    }

    public JvmTask getTask(JVMId jvmId) throws IOException {
      return null;
    }

    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }

    public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      statusUpdate(taskId, taskStatus);
    }
    
    public boolean canCommit(TaskAttemptID taskid) throws IOException {
      return true;
    }
    
    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      StringBuffer buf = new StringBuffer("Task ");
      buf.append(taskId);
      buf.append(" making progress to ");
      buf.append(taskStatus.getProgress());
      String state = taskStatus.getStateString();
      if (state != null) {
        buf.append(" and state of ");
        buf.append(state);
      }
      LOG.info(buf.toString());
      // ignore phase
      // ignore counters
      return true;
    }

    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) throws IOException {
      LOG.info("Task " + taskid + " has problem " + trace);
    }
    
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
        int fromEventId, int maxLocs, TaskAttemptID id) throws IOException {
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY, 
                                               false);
    }

    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }
  }
  
  private static ClassLoader makeClassLoader(JobConf conf, 
                                             File workDir) throws IOException {
    List<URL> cp = new ArrayList<URL>();

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
    return new URLClassLoader(cp.toArray(new URL[cp.size()]));
  }
  
  /**
   * Create empty sequence files for any of the map outputs that we don't have.
   * @param fs the filesystem to create the files in
   * @param dir the directory name to create the files in
   * @param conf the jobconf
   * @throws IOException if something goes wrong writing
   */
  private static void fillInMissingMapOutputs(FileSystem fs, 
                                              TaskAttemptID taskId,
                                              int numMaps,
                                              JobConf conf) throws IOException {
    Class<? extends WritableComparable> keyClass
        = conf.getMapOutputKeyClass().asSubclass(WritableComparable.class);
    Class<? extends Writable> valueClass
        = conf.getMapOutputValueClass().asSubclass(Writable.class);
    MapOutputFile namer = new MapOutputFile(taskId.getJobID());
    namer.setConf(conf);
    for(int i=0; i<numMaps; i++) {
      Path f = namer.getInputFile(i, taskId);
      if (!fs.exists(f)) {
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
  public static void main(String[] args
                          ) throws ClassNotFoundException, IOException, 
                                   InterruptedException {
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
    TaskAttemptID taskId = TaskAttemptID.forName(conf.get("mapred.task.id"));
    boolean isMap = conf.getBoolean("mapred.task.is.map", true);
    int partition = conf.getInt("mapred.task.partition", 0);
    
    // setup the local and user working directories
    FileSystem local = FileSystem.getLocal(conf);
    LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");
    File workDirName = new File(lDirAlloc.getLocalPathToRead(
                                  TaskTracker.getLocalTaskDir(
                                    taskId.getJobID().toString(), 
                                    taskId.toString())
                                  + Path.SEPARATOR + "work",
                                  conf). toString());
    local.setWorkingDirectory(new Path(workDirName.toString()));
    FileSystem.get(conf).setWorkingDirectory(conf.getWorkingDirectory());
    
    // set up a classloader with the right classpath
    ClassLoader classLoader = makeClassLoader(conf, workDirName);
    Thread.currentThread().setContextClassLoader(classLoader);
    conf.setClassLoader(classLoader);
    
    Task task;
    if (isMap) {
      Path localSplit = new Path(new Path(jobFilename.toString()).getParent(), 
                                 "split.dta");
      DataInputStream splitFile = FileSystem.getLocal(conf).open(localSplit);
      String splitClass = Text.readString(splitFile);
      BytesWritable split = new BytesWritable();
      split.readFields(splitFile);
      splitFile.close();
      task = new MapTask(jobFilename.toString(), taskId, partition, splitClass, split);
    } else {
      int numMaps = conf.getNumMapTasks();
      fillInMissingMapOutputs(local, taskId, numMaps, conf);
      task = new ReduceTask(jobFilename.toString(), taskId, partition, numMaps);
    }
    task.setConf(conf);
    task.run(conf, new FakeUmbilical());
  }

}
