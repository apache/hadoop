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

import java.io.*;
import java.util.*;

/**************************************************
 * A TaskTrackerStatus is a MapReduce primitive.  Keeps
 * info on a TaskTracker.  The JobTracker maintains a set
 * of the most recent TaskTrackerStatus objects for each
 * unique TaskTracker it knows about.
 *
 * @author Mike Cafarella
 **************************************************/
class TaskTrackerStatus implements Writable {

    static {                                        // register a ctor
      WritableFactories.setFactory
        (TaskTrackerStatus.class,
         new WritableFactory() {
           public Writable newInstance() { return new TaskTrackerStatus(); }
         });
    }

    String trackerName;
    String host;
    int port;
    Vector taskReports;
    
    volatile long lastSeen;
    
    /**
     */
    public TaskTrackerStatus() {
    }

    /**
     */
    public TaskTrackerStatus(String trackerName, String host, int port, Vector taskReports) {
        this.trackerName = trackerName;
        this.host = host;
        this.port = port;

        this.taskReports = new Vector();
        this.taskReports.addAll(taskReports);
    }

    /**
     */
    public String getTrackerName() {
        return trackerName;
    }
    /**
     */
    public String getHost() {
        return host;
    }
    /**
     */
    public int getPort() {
        return port;
    }

    /**
     * All current tasks at the TaskTracker.  
     *
     * Tasks are tracked by a TaskStatus object.
     */
    public Iterator taskReports() {
        return taskReports.iterator();
    }

    /**
     * Return the current MapTask count
     */
    public int countMapTasks() {
        int mapCount = 0;
        for (Iterator it = taskReports.iterator(); it.hasNext(); ) {
            TaskStatus ts = (TaskStatus) it.next();
            if (ts.getIsMap()) {
                mapCount++;
            }
        }
        return mapCount;
    }

    /**
     * Return the current ReduceTask count
     */
    public int countReduceTasks() {
        return taskReports.size() - countMapTasks();
    }

    /**
     */
    public long getLastSeen() {
        return lastSeen;
    }
    /**
     */
    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    ///////////////////////////////////////////
    // Writable
    ///////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, trackerName);
        UTF8.writeString(out, host);
        out.writeInt(port);

        out.writeInt(taskReports.size());
        for (Iterator it = taskReports.iterator(); it.hasNext(); ) {
            ((TaskStatus) it.next()).write(out);
        }
    }

    /**
     */     
    public void readFields(DataInput in) throws IOException {
        this.trackerName = UTF8.readString(in);
        this.host = UTF8.readString(in);
        this.port = in.readInt();

        taskReports = new Vector();
        taskReports.clear();

        int numTasks = in.readInt();
        for (int i = 0; i < numTasks; i++) {
            TaskStatus tmp = new TaskStatus();
            tmp.readFields(in);
            taskReports.add(tmp);
        }
    }
}
