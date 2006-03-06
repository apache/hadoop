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

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/** Runs a reduce task. */
class ReduceTaskRunner extends TaskRunner {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.mapred.ReduceTaskRunner");
  private MapOutputFile mapOutputFile;

  public ReduceTaskRunner(Task task, TaskTracker tracker, Configuration conf) {
    super(task, tracker, conf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
  }

  /** Assemble all of the map output files. */
  public boolean prepare() throws IOException {
    ReduceTask task = ((ReduceTask)getTask());
    this.mapOutputFile.removeAll(task.getTaskId());    // cleanup from failures
    String[][] mapTaskIds = task.getMapTaskIds();
    final Progress copyPhase = getTask().getProgress().phase();

    // we need input from every map task
    Vector needed = new Vector();
    for (int i = 0; i < mapTaskIds.length; i++) {
      needed.add(mapTaskIds[i]);
      copyPhase.addPhase();                       // add sub-phase per file
    }

    InterTrackerProtocol jobClient = getTracker().getJobClient();
    while (needed.size() > 0) {
      getTask().reportProgress(getTracker());

      // query for a just a random subset of needed segments so that we don't
      // overwhelm jobtracker.  ideally perhaps we could send a more compact
      // representation of all needed, i.e., a bit-vector
      Collections.shuffle(needed);
      int checkSize = Math.min(10, needed.size());
      String[][] neededStrings = new String[checkSize][];
      for (int i = 0; i < checkSize; i++) {
          neededStrings[i] = (String[]) needed.elementAt(i);
      }
      MapOutputLocation[] locs =
        jobClient.locateMapOutputs(task.getTaskId(), neededStrings);

      if (locs.length == 0) {
        try {
          if (killed) {
            return false;
          }
          Thread.sleep(10000);
        } catch (InterruptedException e) {
        }
        continue;
      }

      LOG.info(task.getTaskId()+" Got "+locs.length+" map output locations.");

      // try each of these locations
      for (int i = 0; i < locs.length; i++) {
        MapOutputLocation loc = locs[i];
        InetSocketAddress addr =
          new InetSocketAddress(loc.getHost(), loc.getPort());
        MapOutputProtocol client =
          (MapOutputProtocol)RPC.getProxy(MapOutputProtocol.class, addr, this.conf);

        this.mapOutputFile.setProgressReporter(new MapOutputFile.ProgressReporter() {
            public void progress(float progress) {
              copyPhase.phase().set(progress);
              try {
                getTask().reportProgress(getTracker());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });

        getTask().reportProgress(getTracker());
        try {
          copyPhase.phase().setStatus(loc.toString());
          
          client.getFile(loc.getMapTaskId(), task.getTaskId(),
                         new IntWritable(task.getPartition()));

          // Success: remove from 'needed'
          boolean foundit = false;
          for (Iterator it = needed.iterator(); it.hasNext() && !foundit; ) {
              String idsForSingleMap[] = (String[]) it.next();
              for (int j = 0; j < idsForSingleMap.length; j++) {
                  if (idsForSingleMap[j].equals(loc.getMapTaskId())) {
                      it.remove();
                      foundit = true;
                      break;
                  }
              }
          }
          copyPhase.startNextPhase();
          
        } catch (IOException e) {                 // failed: try again later
          LOG.log(Level.WARNING,
                  task.getTaskId()+" copy failed: "
                  +loc.getMapTaskId()+" from "+addr,
                  e);
        } finally {
          this.mapOutputFile.setProgressReporter(null);
        }
      }
    }
    getTask().reportProgress(getTracker());
    return true;
  }

  /** Delete all of the temporary map output files. */
  public void close() throws IOException {
    getTask().getProgress().setStatus("closed");
    this.mapOutputFile.removeAll(getTask().getTaskId());
  }

}
