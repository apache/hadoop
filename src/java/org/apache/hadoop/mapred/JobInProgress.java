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

import org.apache.hadoop.fs.*;
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
class JobInProgress {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.mapred.JobInProgress");

    JobProfile profile;
    JobStatus status;
    Path localJobFile = null;
    Path localJarFile = null;

    TaskInProgress maps[] = new TaskInProgress[0];
    TaskInProgress reduces[] = new TaskInProgress[0];
    int numMapTasks = 0;
    int numReduceTasks = 0;

    JobTracker jobtracker = null;
    HashMap hostToMaps = new HashMap();

    long startTime;
    long finishTime;

    private JobConf conf;
    private int firstMapToTry = 0;
    private int firstReduceToTry = 0;
    boolean tasksInited = false;

    private LocalFileSystem localFs;
    private String uniqueString;
  
    /**
     * Create a JobInProgress with the given job file, plus a handle
     * to the tracker.
     */
    public JobInProgress(String jobFile, JobTracker jobtracker, 
                         Configuration default_conf) throws IOException {
        uniqueString = jobtracker.createUniqueId();
        String jobid = "job_" + uniqueString;
        String url = "http://" + jobtracker.getJobTrackerMachine() + ":" + jobtracker.getInfoPort() + "/jobdetails.jsp?jobid=" + jobid;
        this.jobtracker = jobtracker;
        this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.PREP);
        this.startTime = System.currentTimeMillis();
        this.localFs = (LocalFileSystem)FileSystem.getNamed("local", default_conf);

        JobConf default_job_conf = new JobConf(default_conf);
        this.localJobFile = default_job_conf.getLocalPath(JobTracker.SUBDIR 
                                                          +"/"+jobid + ".xml");
        this.localJarFile = default_job_conf.getLocalPath(JobTracker.SUBDIR
                                                          +"/"+ jobid + ".jar");
        FileSystem fs = FileSystem.get(default_conf);
        fs.copyToLocalFile(new Path(jobFile), localJobFile);
        conf = new JobConf(localJobFile);
        this.profile = new JobProfile(conf.getUser(), jobid, jobFile, url,
                                      conf.getJobName());
        String jarFile = conf.getJar();
        if (jarFile != null) {
          fs.copyToLocalFile(new Path(jarFile), localJarFile);
          conf.setJar(localJarFile.toString());
        }

        this.numMapTasks = conf.getNumMapTasks();
        this.numReduceTasks = conf.getNumReduceTasks();
    }

    /**
     * Construct the splits, etc.  This is invoked from an async
     * thread so that split-computation doesn't block anyone.
     */
    public synchronized void initTasks() throws IOException {
        if (tasksInited) {
            return;
        }

        //
        // construct input splits
        //
        String jobFile = profile.getJobFile();

        JobConf jd = new JobConf(localJobFile);
        FileSystem fs = FileSystem.get(conf);
        String ifClassName = jd.get("mapred.input.format.class");
        InputFormat inputFormat;
        if (ifClassName != null && localJarFile != null) {
          try {
            ClassLoader loader =
              new URLClassLoader(new URL[]{ localFs.pathToFile(localJarFile).toURL() });
            Class inputFormatClass = loader.loadClass(ifClassName);
            inputFormat = (InputFormat)inputFormatClass.newInstance();
          } catch (Exception e) {
            throw new IOException(e.toString());
          }
        } else {
          inputFormat = jd.getInputFormat();
        }

        FileSplit[] splits = inputFormat.getSplits(fs, jd, numMapTasks);

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
            maps[i] = new TaskInProgress(uniqueString, jobFile, splits[i], 
                                         jobtracker, conf, this, i);
        }

        //
        // Create reduce tasks
        //
        this.reduces = new TaskInProgress[numReduceTasks];
        for (int i = 0; i < numReduceTasks; i++) {
            reduces[i] = new TaskInProgress(uniqueString, jobFile, 
                                            numMapTasks, i, 
                                            jobtracker, conf, this);
        }

        //
        // Obtain some tasktracker-cache information for the map task splits.
        //
        for (int i = 0; i < maps.length; i++) {
            String hints[][] =
              fs.getFileCacheHints(splits[i].getPath(), splits[i].getStart(),
                                   splits[i].getLength());

            if (hints != null) {
              for (int k = 0; k < hints.length; k++) {
                for (int j = 0; j < hints[k].length; j++) {
                  ArrayList hostMaps = (ArrayList)hostToMaps.get(hints[k][j]);
                  if (hostMaps == null) {
                    hostMaps = new ArrayList();
                    hostToMaps.put(hints[k][j], hostMaps);
                  }
                  hostMaps.add(maps[i]);
                }
              }
            }
        }

        this.status = new JobStatus(status.getJobId(), 0.0f, 0.0f, JobStatus.RUNNING);
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
     * Get the list of map tasks
     * @return the raw array of maps for this job
     */
    TaskInProgress[] getMapTasks() {
      return maps;
    }
    
    /**
     * Get the list of reduce tasks
     * @return the raw array of reduce tasks for this job
     */
    TaskInProgress[] getReduceTasks() {
      return reduces;
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
    public synchronized void updateTaskStatus(TaskInProgress tip, 
                                              TaskStatus status) {
        double oldProgress = tip.getProgress();   // save old progress
        tip.updateStatus(status);                 // update tip
        LOG.fine("Taking progress for " + tip.getTIPId() + " from " + 
                 oldProgress + " to " + tip.getProgress());

        //
        // Update JobInProgress status
        //
        double progressDelta = tip.getProgress() - oldProgress;
        if (tip.isMapTask()) {
          if (maps.length == 0) {
            this.status.setMapProgress(1.0f);
          } else {
            this.status.mapProgress += (progressDelta / maps.length);
          }
        } else {
          if (reduces.length == 0) {
            this.status.setReduceProgress(1.0f);
          } else {
            this.status.reduceProgress += (progressDelta / reduces.length);
          }
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
            LOG.info("Cannot create task split for " + profile.getJobId());
            return null;
        }

        Task t = null;
        int cacheTarget = -1;
        int stdTarget = -1;
        int specTarget = -1;
        int failedTarget = -1;

        //
        // We end up creating two tasks for the same bucket, because
        // we call obtainNewMapTask() really fast, twice in a row.
        // There's not enough time for the "recentTasks"
        //

        //
        // Compute avg progress through the map tasks
        //
        double avgProgress = status.mapProgress() / maps.length;

        //
        // See if there is a split over a block that is stored on
        // the TaskTracker checking in.  That means the block
        // doesn't have to be transmitted from another node.
        //
        ArrayList hostMaps = (ArrayList)hostToMaps.get(tts.getHost());
        if (hostMaps != null) {
          Iterator i = hostMaps.iterator();
          while (i.hasNext()) {
            TaskInProgress tip = (TaskInProgress)i.next();
            if (tip.hasTask() && !tip.hasFailedOnMachine(taskTracker)) {
              LOG.info("Found task with local split for "+tts.getHost());
              cacheTarget = tip.getIdWithinJob();
              i.remove();
              break;
            }
          }
        }

        //
        // If there's no cached target, see if there's
        // a std. task to run.
        //
        if (cacheTarget < 0) {
            for (int i = 0; i < maps.length; i++) {
                int realIdx = (i + firstMapToTry) % maps.length; 
                if (maps[realIdx].hasTask()) {
                    if (stdTarget < 0) {
                      if (maps[realIdx].hasFailedOnMachine(taskTracker)) {
                        if (failedTarget < 0) {
                          failedTarget = realIdx;
                        }
                      } else {
                        stdTarget = realIdx;
                        break;
                      }
                    }
                }
            }
        }

        //
        // If no cached-target and no std target, see if
        // there's a speculative task to run.
        //
        if (cacheTarget < 0 && stdTarget < 0) {
            for (int i = 0; i < maps.length; i++) {        
                int realIdx = (i + firstMapToTry) % maps.length; 
                if (maps[realIdx].hasSpeculativeTask(avgProgress)) {
                      if (!maps[realIdx].hasFailedOnMachine(taskTracker)) {
                        specTarget = realIdx;
                        break;
                      }
                }
            }
        }

        //
        // Run whatever we found
        //
        if (cacheTarget >= 0) {
            t = maps[cacheTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (stdTarget >= 0) {
            t = maps[stdTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (specTarget >= 0) {
            t = maps[specTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (failedTarget >= 0) {
            t = maps[failedTarget].getTaskToRun(taskTracker, tts, avgProgress);
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
            LOG.info("Cannot create task split for " + profile.getJobId());
            return null;
        }

        Task t = null;
        int stdTarget = -1;
        int specTarget = -1;
        int failedTarget = -1;
        double avgProgress = status.reduceProgress() / reduces.length;

        for (int i = 0; i < reduces.length; i++) {
            int realIdx = (i + firstReduceToTry) % reduces.length;
            if (reduces[realIdx].hasTask()) {
                if (reduces[realIdx].hasFailedOnMachine(taskTracker)) {
                  if (failedTarget < 0) {
                    failedTarget = realIdx;
                  }
                } else if (stdTarget < 0) {
                    stdTarget = realIdx;
                }
            } else if (reduces[realIdx].hasSpeculativeTask(avgProgress)) {
                if (specTarget < 0 &&
                    !reduces[realIdx].hasFailedOnMachine(taskTracker)) {
                    specTarget = realIdx;
                }
            }
        }
        
        if (stdTarget >= 0) {
            t = reduces[stdTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (specTarget >= 0) {
            t = reduces[specTarget].getTaskToRun(taskTracker, tts, avgProgress);
        } else if (failedTarget >= 0) {
            t = reduces[failedTarget].getTaskToRun(taskTracker, tts, 
                                                   avgProgress);
        }
        return t;
    }

    /**
     * A taskid assigned to this JobInProgress has reported in successfully.
     */
    public synchronized void completedTask(TaskInProgress tip, 
                                           TaskStatus status) {
        String taskid = status.getTaskId();
        updateTaskStatus(tip, status);
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
            this.status = new JobStatus(this.status.getJobId(), 1.0f, 1.0f, 
                                        JobStatus.SUCCEEDED);
            this.finishTime = System.currentTimeMillis();
            garbageCollect();
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

            garbageCollect();
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
    public synchronized void failedTask(TaskInProgress tip, String taskid, 
                                        TaskStatus status, String trackerName) {
        tip.failedSubTask(taskid, trackerName);
        updateTaskStatus(tip, status);
        
        // After this, try to assign tasks with the one after this, so that
        // the failed task goes to the end of the list.
        if (tip.isMapTask()) {
          firstMapToTry = (tip.getIdWithinJob() + 1) % maps.length;
        } else {
          firstReduceToTry = (tip.getIdWithinJob() + 1) % reduces.length;
        }
            
        //
        // Check if we need to kill the job because of too many failures
        //
        if (tip.isFailed()) {
            LOG.info("Aborting job " + profile.getJobId());
            kill();
        }

        jobtracker.removeTaskEntry(taskid);
 }

    /**
     * Fail a task with a given reason, but without a status object.
     * @author Owen O'Malley
     * @param tip The task's tip
     * @param taskid The task id
     * @param reason The reason that the task failed
     * @param trackerName The task tracker the task failed on
     */
    public void failedTask(TaskInProgress tip, String taskid, 
                           String reason, String hostname, String trackerName) {
       TaskStatus status = new TaskStatus(taskid,
                                          tip.isMapTask(),
                                          0.0f,
                                          TaskStatus.FAILED,
                                          reason,
                                          reason,
                                          hostname);
       failedTask(tip, taskid, status, trackerName);
    }
       
                           
    /**
     * The job is dead.  We're now GC'ing it, getting rid of the job
     * from all tables.  Be sure to remove all of this job's tasks
     * from the various tables.
     */
    synchronized void garbageCollect() {
      try {
        // Definitely remove the local-disk copy of the job file
        if (localJobFile != null) {
            localFs.delete(localJobFile);
            localJobFile = null;
        }
        if (localJarFile != null) {
            localFs.delete(localJarFile);
            localJarFile = null;
        }

        // JobClient always creates a new directory with job files
        // so we remove that directory to cleanup
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(profile.getJobFile()).getParent());

      } catch (IOException e) {
        LOG.warning("Error cleaning up "+profile.getJobId()+": "+e);
      }
    }

    /**
      * Return the TaskInProgress that matches the tipid.
      */
    public TaskInProgress getTaskInProgress(String tipid){
      for (int i = 0; i < maps.length; i++) {
        if (tipid.equals(maps[i].getTIPId())){
          return maps[i];
        }               
      }
      for (int i = 0; i < reduces.length; i++) {
        if (tipid.equals(reduces[i].getTIPId())){
          return reduces[i];
        }
      }
      return null;
    }
    
    /**
     * Find the details of someplace where a map has finished
     * @param mapId the id of the map
     * @return the task status of the completed task
     */
    public TaskStatus findFinishedMap(int mapId) {
       TaskInProgress tip = maps[mapId];
       if (tip.isComplete()) {
         TaskStatus[] statuses = tip.getTaskStatuses();
         for(int i=0; i < statuses.length; i++) {
           if (statuses[i].getRunState() == TaskStatus.SUCCEEDED) {
             return statuses[i];
           }
         }
       }
       return null;
    }
}
