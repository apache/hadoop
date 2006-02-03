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
package org.apache.hadoop.mapred.

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

///////////////////////////////////////////////////////
// JobInProgress maintains all the info for keeping
// a Job on the straight and narrow.  It keeps its JobProfile
// and its latest JobStatus, plus a set of tables for 
// doing bookkeeping of its Tasks.
///////////////////////////////////////////////////////
public class JobInProgress {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.mapred.JobInProgress");

    JobProfile profile;
    JobStatus status;
    File localJobFile = null;

    TaskInProgress maps[] = null;
    TaskInProgress reduces[] = null;
    int numMapTasks = 0;
    int numReduceTasks = 0;

    JobTracker jobtracker = null;

    long startTime;
    long finishTime;
    String deleteUponCompletion = null;

    Configuration conf;
    boolean tasksInited = false;

    /**
     * Create a JobInProgress with the given job file, plus a handle
     * to the tracker.
     */
    public JobInProgress(String jobFile, JobTracker jobtracker, Configuration conf) throws IOException {
        String jobid = "job_" + jobtracker.createUniqueId();
        String url = "http://" + jobtracker.getJobTrackerMachine() + ":" + jobtracker.getInfoPort() + "/jobdetails.jsp?jobid=" + jobid;
        this.conf = conf;
        this.jobtracker = jobtracker;
        this.profile = new JobProfile(jobid, jobFile, url);
        this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.RUNNING);
        this.startTime = System.currentTimeMillis();

        this.localJobFile = new JobConf(conf).getLocalFile(JobTracker.SUBDIR, jobid + ".xml");
        NutchFileSystem fs = NutchFileSystem.get(conf);
        fs.copyToLocalFile(new File(jobFile), localJobFile);

        JobConf jd = new JobConf(localJobFile);
        this.numMapTasks = jd.getNumMapTasks();
        this.numReduceTasks = jd.getNumReduceTasks();

        //
        // If a jobFile is in the systemDir, we can delete it (and
        // its JAR) upon completion
        //
        File systemDir = jd.getSystemDir();
        if (jobFile.startsWith(systemDir.getPath())) {
            this.deleteUponCompletion = jobFile;
        }
    }

    /**
     * Construct the splits, etc
     */
    void initTasks() throws IOException {
        if (tasksInited) {
            return;
        }

        //
        // construct input splits
        //
        String jobid = profile.getJobId();
        String jobFile = profile.getJobFile();

        JobConf jd = new JobConf(localJobFile);
        NutchFileSystem fs = NutchFileSystem.get(conf);
        FileSplit[] splits =
            jd.getInputFormat().getSplits(fs, jd, numMapTasks);

        //
        // sort splits by decreasing length, to reduce job's tail
        //
        Arrays.sort(splits, new Comparator() {
            public int compare(Object a, Object b) {
                long diff =
                    ((FileSplit)b).getLength() - ((FileSplit)a).getLength();
                return diff==0 ? 0 : (diff > 0 ? 1 : -1);
            }
        });

        //
        // adjust number of map tasks to actual number of splits
        //
        this.numMapTasks = splits.length;
        // create a map task for each split
        this.maps = new TaskInProgress[numMapTasks];
        for (int i = 0; i < numMapTasks; i++) {
            maps[i] = new TaskInProgress(jobFile, splits[i], jobtracker, conf, this);
        }

        //
        // Create reduce tasks
        //
        this.reduces = new TaskInProgress[numReduceTasks];
        for (int i = 0; i < numReduceTasks; i++) {
            reduces[i] = new TaskInProgress(jobFile, maps, i, jobtracker, conf, this);
        }

        tasksInited = true;
    }

    /////////////////////////////////////////////////////
    // Accessors for the JobInProgress
    /////////////////////////////////////////////////////
    public JobProfile getProfile() {
        return profile;
    }
    public JobStatus getStatus() {
        return status;
    }
    public long getStartTime() {
        return startTime;
    }
    public long getFinishTime() {
        return finishTime;
    }
    public int desiredMaps() {
        return numMapTasks;
    }
    public int finishedMaps() {
        int finishedCount = 0;
        for (int i = 0; i < maps.length; i++) {
            if (maps[i].isComplete()) {
                finishedCount++;
            }
        }
        return finishedCount;
    }
    public int desiredReduces() {
        return numReduceTasks;
    }
    public int finishedReduces() {
        int finishedCount = 0;
        for (int i = 0; i < reduces.length; i++) {
            if (reduces[i].isComplete()) {
                finishedCount++;
            }
        }
        return finishedCount;
    }

    /**
     * Return a treeset of completed TaskInProgress objects
     */
    public Vector reportTasksInProgress(boolean shouldBeMap, boolean shouldBeComplete) {
        Vector results = new Vector();
        TaskInProgress tips[] = null;
        if (shouldBeMap) {
            tips = maps;
        } else {
            tips = reduces;
        }
        for (int i = 0; i < tips.length; i++) {
            if (tips[i].isComplete() == shouldBeComplete) {
                results.add(tips[i]);
            }
        }
        return results;
    }

    ////////////////////////////////////////////////////
    // Status update methods
    ////////////////////////////////////////////////////
    public void updateTaskStatus(TaskInProgress tip, TaskStatus status) {
        tip.updateStatus(status);

        //
        // Update JobInProgress status
        //
        if (maps.length == 0) {
            this.status.setMapProgress(1.0f);
        } else {
            double reportedProgress = 0;
            for (int i = 0; i < maps.length; i++) {
                reportedProgress += maps[i].getProgress();
            }
            this.status.setMapProgress((float) (reportedProgress / maps.length));
        }
        if (reduces.length == 0) {
            this.status.setReduceProgress(1.0f);
        } else {
            double reportedProgress = 0;
            for (int i = 0; i < reduces.length; i++) {
                reportedProgress += reduces[i].getProgress();
            }
            this.status.setReduceProgress((float) (reportedProgress / reduces.length));
        }
    }

    /////////////////////////////////////////////////////
    // Create/manage tasks
    /////////////////////////////////////////////////////
    /**
     * Return a MapTask, if appropriate, to run on the given tasktracker
     */
    public Task obtainNewMapTask(String taskTracker, TaskTrackerStatus tts) {
        if (! tasksInited) {
            try {
                initTasks();
            } catch (IOException ie) {
                ie.printStackTrace();
                LOG.info("Cannot create task split for " + profile.getJobId());
            }
        }

        Task t = null;
        int cacheTarget = -1;
        int stdTarget = -1;
        int specTarget = -1;
        double totalProgress = 0;

        //
        // We end up creating two tasks for the same bucket, because
        // we call obtainNewMapTask() really fast, twice in a row.
        // There's not enough time for the "recentTasks"
        //
        for (int i = 0; i < maps.length; i++) {
            if (maps[i].hasTaskWithCacheHit(taskTracker, tts)) {
                if (cacheTarget < 0) {
                    cacheTarget = i;
                }
            } else if (maps[i].hasTask()) {
                if (stdTarget < 0) {
                    stdTarget = i;
                }
            }
            totalProgress += maps[i].getProgress();
        }
        double avgProgress = totalProgress / maps.length;

        for (int i = 0; i < maps.length; i++) {        
            if (maps[i].hasSpeculativeTask(avgProgress)) {
                if (specTarget < 0) {
                    specTarget = i;
                }
            }
        }
        
        if (cacheTarget >= 0) {
            t = maps[cacheTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (stdTarget >= 0) {
            t = maps[stdTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (specTarget >= 0) {
            t = maps[specTarget].getTaskToRun(taskTracker, tts, avgProgress);
        }
        return t;
    }

    /**
     * Return a ReduceTask, if appropriate, to run on the given tasktracker.
     * We don't have cache-sensitivity for reduce tasks, as they
     *  work on temporary MapRed files.  
     */
    public Task obtainNewReduceTask(String taskTracker, TaskTrackerStatus tts) {
        if (! tasksInited) {
            try {
                initTasks();
            } catch (IOException ie) {
                ie.printStackTrace();
                LOG.info("Cannot create task split for " + profile.getJobId());
            }
        }

        Task t = null;
        int stdTarget = -1;
        int specTarget = -1;
        int totalProgress = 0;
        for (int i = 0; i < reduces.length; i++) {
            totalProgress += reduces[i].getProgress();
        }
        double avgProgress = (1.0 * totalProgress) / reduces.length;

        for (int i = 0; i < reduces.length; i++) {
            if (reduces[i].hasTask()) {
                if (stdTarget < 0) {
                    stdTarget = i;
                }
            } else if (reduces[i].hasSpeculativeTask(avgProgress)) {
                if (specTarget < 0) {
                    specTarget = i;
                }
            }
        }
        
        if (stdTarget >= 0) {
            t = reduces[stdTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (specTarget >= 0) {
            t = reduces[specTarget].getTaskToRun(taskTracker, tts, avgProgress);
        }
        return t;
    }

    /**
     * A taskid assigned to this JobInProgress has reported in successfully.
     */
    public synchronized void completedTask(TaskInProgress tip, String taskid) {
        LOG.info("Taskid '" + taskid + "' has finished successfully.");
        tip.completed(taskid);

        //
        // Figure out whether the Job is done
        //
        boolean allDone = true;
        for (int i = 0; i < maps.length; i++) {
            if (! maps[i].isComplete()) {
                allDone = false;
                break;
            }
        }
        if (allDone) {
            for (int i = 0; i < reduces.length; i++) {
                if (! reduces[i].isComplete()) {
                    allDone = false;
                    break;
                }
            }
        }

        //
        // If all tasks are complete, then the job is done!
        //
        if (status.getRunState() == JobStatus.RUNNING && allDone) {
            this.status = new JobStatus(status.getJobId(), 1.0f, 1.0f, JobStatus.SUCCEEDED);
            this.finishTime = System.currentTimeMillis();
        }
    }

    /**
     * Kill the job and all its component tasks.
     */
    public synchronized void kill() {
        if (status.getRunState() != JobStatus.FAILED) {
            this.status = new JobStatus(status.getJobId(), 1.0f, 1.0f, JobStatus.FAILED);
            this.finishTime = System.currentTimeMillis();

            //
            // kill all TIPs.
            //
            for (int i = 0; i < maps.length; i++) {
                maps[i].kill();
            }
            for (int i = 0; i < reduces.length; i++) {
                reduces[i].kill();
            }
        }
    }

    /**
     * A task assigned to this JobInProgress has reported in as failed.
     * Most of the time, we'll just reschedule execution.  However, after
     * many repeated failures we may instead decide to allow the entire 
     * job to fail.
     *
     * Even if a task has reported as completed in the past, it might later
     * be reported as failed.  That's because the TaskTracker that hosts a map
     * task might die before the entire job can complete.  If that happens,
     * we need to schedule reexecution so that downstream reduce tasks can 
     * obtain the map task's output.
     */
    public void failedTask(TaskInProgress tip, String taskid, String trackerName) {
        tip.failedSubTask(taskid, trackerName);
            
        //
        // Check if we need to kill the job because of too many failures
        //
        if (tip.isFailed()) {
            LOG.info("Aborting job " + profile.getJobId());
            kill();
        }
    }

    /**
     * The job is dead.  We're now GC'ing it, getting rid of the job
     * from all tables.  Be sure to remove all of this job's tasks
     * from the various tables.
     */
    public synchronized void garbageCollect() throws IOException {
        //
        // Remove this job from all tables
        //

        // Definitely remove the local-disk copy of the job file
        if (localJobFile != null) {
            localJobFile.delete();
            localJobFile = null;
        }

        //
        // If the job file was in the temporary system directory,
        // we should delete it upon garbage collect.
        //
        if (deleteUponCompletion != null) {
            JobConf jd = new JobConf(deleteUponCompletion);
            NutchFileSystem fs = NutchFileSystem.get(conf);
            fs.delete(new File(jd.getJar()));
            fs.delete(new File(deleteUponCompletion));
            deleteUponCompletion = null;
        }
    }
}

