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
import java.util.logging.Level;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.TaskTracker.MapOutputServer;

/** A local file to be transferred via the {@link MapOutputProtocol}. */ 
class MapOutputFile implements Writable, Configurable {

    static {                                      // register a ctor
      WritableFactories.setFactory
        (MapOutputFile.class,
         new WritableFactory() {
           public Writable newInstance() { return new MapOutputFile(); }
         });
    }

  private String mapTaskId;
  private String reduceTaskId;
  private int partition;
  
  /** Permits reporting of file copy progress. */
  public interface ProgressReporter {
    void progress(float progress) throws IOException;
  }

  private ThreadLocal REPORTERS = new ThreadLocal();
  private JobConf jobConf;
  
  public void setProgressReporter(ProgressReporter reporter) {
    REPORTERS.set(reporter);
  }

  /** Create a local map output file name.
   * @param mapTaskId a map task id
   * @param partition a reduce partition
   */
  public File getOutputFile(String mapTaskId, int partition)
    throws IOException {
    return this.jobConf.getLocalFile(mapTaskId, "part-"+partition+".out");
  }

  /** Create a local reduce input file name.
   * @param mapTaskId a map task id
   * @param reduceTaskId a reduce task id
   */
  public File getInputFile(String mapTaskId, String reduceTaskId)
    throws IOException {
    return this.jobConf.getLocalFile(reduceTaskId, mapTaskId+".out");
  }
  public File getInputFile(String mapTaskIds[], String reduceTaskId)
    throws IOException {
    for (int i = 0; i < mapTaskIds.length; i++) {
      File file = jobConf.getLocalFile(reduceTaskId, mapTaskIds[i]+".out");
      if (file.exists())
        return file;
    }
    throw new IOException("Input file not found!");
  }

  /** Removes all of the files related to a task. */
  public void removeAll(String taskId) throws IOException {
    this.jobConf.deleteLocalFiles(taskId);
  }

  /** 
   * Removes all contents of temporary storage.  Called upon 
   * startup, to remove any leftovers from previous run.
   */
  public void cleanupStorage() throws IOException {
    this.jobConf.deleteLocalFiles();
  }

  /** Construct a file for transfer. */
  public MapOutputFile() { 
  }
  
  public MapOutputFile(String mapTaskId, String reduceTaskId, int partition) {
    this.mapTaskId = mapTaskId;
    this.reduceTaskId = reduceTaskId;
    this.partition = partition;
  }

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, mapTaskId);
    UTF8.writeString(out, reduceTaskId);
    out.writeInt(partition);
    
    File file = getOutputFile(mapTaskId, partition);
    FSDataInputStream in = null;
    try {
      // write the length-prefixed file content to the wire
      out.writeLong(file.length());
      in = FileSystem.getNamed("local", this.jobConf).open(file);
    } catch (FileNotFoundException e) {
      TaskTracker.LOG.log(Level.SEVERE, "Can't open map output:" + file, e);
      ((MapOutputServer)Server.get()).getTaskTracker().mapOutputLost(mapTaskId);
      throw e;
    }
    try {
      byte[] buffer = new byte[8192];
      int l  = 0;
      
      while (l != -1) {
        out.write(buffer, 0, l);
        try {
          l = in.read(buffer);
        } catch (IOException e) {
          TaskTracker.LOG.log(Level.SEVERE,"Can't read map output:" + file, e);
          ((MapOutputServer)Server.get()).getTaskTracker().mapOutputLost(mapTaskId);
          throw e;
        }
      }
    } finally {
      in.close();
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.mapTaskId = UTF8.readString(in);
    this.reduceTaskId = UTF8.readString(in);
    this.partition = in.readInt();

    ProgressReporter reporter = (ProgressReporter)REPORTERS.get();

    // read the length-prefixed file content into a local file
    File file = getInputFile(mapTaskId, reduceTaskId);
    long length = in.readLong();
    float progPerByte = 1.0f / length;
    long unread = length;
    FSDataOutputStream out = FileSystem.getNamed("local", this.jobConf).create(file);
    try {
      byte[] buffer = new byte[8192];
      while (unread > 0) {
          int bytesToRead = (int)Math.min(unread, buffer.length);
          in.readFully(buffer, 0, bytesToRead);
          out.write(buffer, 0, bytesToRead);
          unread -= bytesToRead;
          if (reporter != null) {
            reporter.progress((length-unread)*progPerByte);
          }
      }
    } finally {
      out.close();
    }
  }

  public void setConf(Configuration conf) {
    this.jobConf = new JobConf(conf);
  }

  public Configuration getConf() {
    return this.jobConf;
  }

}
