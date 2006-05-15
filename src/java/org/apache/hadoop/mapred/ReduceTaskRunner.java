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

import org.apache.hadoop.ipc.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/** Runs a reduce task. */
class ReduceTaskRunner extends TaskRunner {
  private MapOutputFile mapOutputFile;

  public ReduceTaskRunner(Task task, TaskTracker tracker, JobConf conf) {
    super(task, tracker, conf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
  }

  /** Assemble all of the map output files. */
  public boolean prepare() throws IOException {
    ReduceTask task = ((ReduceTask)getTask());
    this.mapOutputFile.removeAll(task.getTaskId());    // cleanup from failures
    int numMaps = task.getNumMaps();
    final Progress copyPhase = getTask().getProgress().phase();

    // we need input from every map task
    List needed = new ArrayList(numMaps);
    for (int i = 0; i < numMaps; i++) {
      needed.add(new Integer(i));
      copyPhase.addPhase();                       // add sub-phase per file
    }

    InterTrackerProtocol jobClient = getTracker().getJobClient();
    while (needed.size() > 0) {
      LOG.info(task.getTaskId()+" Need "+needed.size()+" map output(s).");
      getTask().reportProgress(getTracker());

      // query for a just a random subset of needed segments so that we don't
      // overwhelm jobtracker.  ideally perhaps we could send a more compact
      // representation of all needed, i.e., a bit-vector
      int checkSize = Math.min(10, needed.size());
      int[] neededIds = new int[checkSize];
      Collections.shuffle(needed);
      ListIterator itr = needed.listIterator();
      for (int i = 0; i < checkSize; i++) {
        neededIds[i] = ((Integer) itr.next()).intValue();
      }
      MapOutputLocation[] locs = null;
      try {
        locs = jobClient.locateMapOutputs(task.getJobId().toString(), 
                                          neededIds, task.getPartition());
      } catch (IOException ie) {
        LOG.info("Problem locating map outputs: " + 
                 StringUtils.stringifyException(ie));
      }
      if (locs == null || locs.length == 0) {
        try {
          if (killed) {
            return false;
          }
          LOG.info(task.getTaskId()+" No map outputs available; sleeping...");
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
          
          LOG.info(task.getTaskId()+" Copying "+loc.getMapTaskId()
                   +" output from "+loc.getHost()+".");
          client.getFile(loc.getMapTaskId(), task.getTaskId(),
                         loc.getMapId(),
                         task.getPartition());

          // Success: remove from 'needed'
          for (Iterator it = needed.iterator(); it.hasNext(); ) {
              int mapId = ((Integer) it.next()).intValue();
              if (mapId == loc.getMapId()) {
                it.remove();
                break;
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
