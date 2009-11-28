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
package org.apache.hadoop.vaidya.statistics.job;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;

/**
 *
 */
public class JobStatistics implements JobStatisticsInterface {
  
  
  /*
   * Pattern for parsing the COUNTERS
   */
  private static final Pattern _pattern = Pattern.compile("[[^,]?]+");  //"[[^,]?]+"
  
  /*
   * Job configuration
   */
  private JobConf _jobConf;
  
  /**
   * @param jobConf the jobConf to set
   */
  void setJobConf(JobConf jobConf) {
    this._jobConf = jobConf;
    // TODO: Add job conf to _job array 
  }

  /*
   * Aggregated Job level counters 
   */
  private JobHistoryParser.JobInfo _jobInfo;
  
  /*
   * Job stats 
   */
  private java.util.Hashtable<Enum, String> _job;

  /**
   * @param jobConf the jobConf to set
   */
  public JobConf getJobConf() {
    return this._jobConf;
  }
  
  /*
   * Get Job Counters of type long
   */
  public long getLongValue(Enum key) {
    if (this._job.get(key) == null) {
      return (long)0;
    }
    else {
      return Long.parseLong(this._job.get(key));
    }
  }
  
  /*
   * Get job Counters of type Double
   */
  public double getDoubleValue(Enum key) {
    if (this._job.get(key) == null) {
      return (double)0;
    } else {
      return Double.parseDouble(this._job.get(key));
    }
  }
  
  /* 
   * Get Job Counters of type String
   */
  public String getStringValue(Enum key) {
  if (this._job.get(key) == null) {
    return "";
  } else {
      return this._job.get(key);
  }
  }
  
  /*
   * Set key value of type long
   */
  public void setValue(Enum key, long value) {
    this._job.put(key, Long.toString(value));
  }
  
  /*
   * Set key value of type double
   */
  public void setValue(Enum key, double value) {
    this._job.put(key, Double.toString(value));
  }
  
  /*
   * Set key value of type String
   */
  public void setValue(Enum key, String value) {
    this._job.put(key, value);
  }

  /*
   * Map Task List (Sorted by task id)
   */
  private ArrayList<MapTaskStatistics> _mapTaskList = new ArrayList<MapTaskStatistics>();
  
  /*
   * Reduce Task List (Sorted by task id)
   */
  private ArrayList<ReduceTaskStatistics> _reduceTaskList = new ArrayList<ReduceTaskStatistics>();

  
  /* 
   * Ctor:
   */
  public JobStatistics (JobConf jobConf, JobInfo jobInfo) throws ParseException {
    this._jobConf = jobConf;
    this._jobInfo = jobInfo;
    this._job = new Hashtable<Enum, String>();
    populate_Job(this._job, jobInfo);  
    populate_MapReduceTaskLists(this._mapTaskList, this._reduceTaskList, 
        jobInfo.getAllTasks());

    // Add the Job Type: MAP_REDUCE, MAP_ONLY
    if (getLongValue(JobKeys.TOTAL_REDUCES) == 0) {
      this._job.put(JobKeys.JOBTYPE,"MAP_ONLY");
    } else {
      this._job.put(JobKeys.JOBTYPE,"MAP_REDUCE");
    }
  }
  
  /*
   * 
   */
  private void populate_MapReduceTaskLists (ArrayList<MapTaskStatistics> mapTaskList, 
                     ArrayList<ReduceTaskStatistics> reduceTaskList, 
                     Map<TaskID, TaskInfo> taskMap)
  throws ParseException {
    int num_tasks = taskMap.entrySet().size();
// DO we need these lists?
//    List<TaskAttemptInfo> successfulMapAttemptList = 
//      new ArrayList<TaskAttemptInfo>();
//    List<TaskAttemptInfo> successfulReduceAttemptList = 
//      new ArrayList<TaskAttemptInfo>();
    for (JobHistoryParser.TaskInfo taskInfo: taskMap.values()) {
      if (taskInfo.getTaskType().equals(TaskType.MAP)) {
        MapTaskStatistics mapT = new MapTaskStatistics();
        TaskAttemptInfo successfulAttempt  =  
          getLastSuccessfulTaskAttempt(taskInfo);
        mapT.setValue(MapTaskKeys.TASK_ID, 
            successfulAttempt.getAttemptId().getTaskID().toString()); 
        mapT.setValue(MapTaskKeys.ATTEMPT_ID, 
            successfulAttempt.getAttemptId().toString()); 
        mapT.setValue(MapTaskKeys.HOSTNAME, 
            successfulAttempt.getTrackerName()); 
        mapT.setValue(MapTaskKeys.TASK_TYPE, 
            successfulAttempt.getTaskType().toString()); 
        mapT.setValue(MapTaskKeys.STATUS, 
            successfulAttempt.getTaskStatus().toString()); 
        mapT.setValue(MapTaskKeys.START_TIME, successfulAttempt.getStartTime()); 
        mapT.setValue(MapTaskKeys.FINISH_TIME, successfulAttempt.getFinishTime()); 
        mapT.setValue(MapTaskKeys.SPLITS, taskInfo.getSplitLocations()); 
        mapT.setValue(MapTaskKeys.TRACKER_NAME, successfulAttempt.getTrackerName()); 
        mapT.setValue(MapTaskKeys.STATE_STRING, successfulAttempt.getState()); 
        mapT.setValue(MapTaskKeys.HTTP_PORT, successfulAttempt.getHttpPort()); 
        mapT.setValue(MapTaskKeys.ERROR, successfulAttempt.getError()); 
        parseAndAddMapTaskCounters(mapT, 
            successfulAttempt.getCounters().toString());
        mapTaskList.add(mapT);

        // Add number of task attempts
        mapT.setValue(MapTaskKeys.NUM_ATTEMPTS, 
            (new Integer(taskInfo.getAllTaskAttempts().size())).toString());

        // Add EXECUTION_TIME = FINISH_TIME - START_TIME
        long etime = mapT.getLongValue(MapTaskKeys.FINISH_TIME) - 
          mapT.getLongValue(MapTaskKeys.START_TIME);
        mapT.setValue(MapTaskKeys.EXECUTION_TIME, (new Long(etime)).toString());

      }else if (taskInfo.getTaskType().equals(TaskType.REDUCE)) {

        ReduceTaskStatistics reduceT = new ReduceTaskStatistics();
        TaskAttemptInfo successfulAttempt  = 
          getLastSuccessfulTaskAttempt(taskInfo);
        reduceT.setValue(ReduceTaskKeys.TASK_ID,
            successfulAttempt.getAttemptId().getTaskID().toString()); 
        reduceT.setValue(ReduceTaskKeys.ATTEMPT_ID,
            successfulAttempt.getAttemptId().toString()); 
        reduceT.setValue(ReduceTaskKeys.HOSTNAME,
            successfulAttempt.getTrackerName()); 
        reduceT.setValue(ReduceTaskKeys.TASK_TYPE, 
            successfulAttempt.getTaskType().toString()); 
        reduceT.setValue(ReduceTaskKeys.STATUS, 
            successfulAttempt.getTaskStatus().toString()); 
        reduceT.setValue(ReduceTaskKeys.START_TIME,
            successfulAttempt.getStartTime()); 
        reduceT.setValue(ReduceTaskKeys.FINISH_TIME,
            successfulAttempt.getFinishTime()); 
        reduceT.setValue(ReduceTaskKeys.SHUFFLE_FINISH_TIME,
            successfulAttempt.getShuffleFinishTime()); 
        reduceT.setValue(ReduceTaskKeys.SORT_FINISH_TIME,
            successfulAttempt.getSortFinishTime()); 
        reduceT.setValue(ReduceTaskKeys.SPLITS, ""); 
        reduceT.setValue(ReduceTaskKeys.TRACKER_NAME,
            successfulAttempt.getTrackerName()); 
        reduceT.setValue(ReduceTaskKeys.STATE_STRING,
            successfulAttempt.getState()); 
        reduceT.setValue(ReduceTaskKeys.HTTP_PORT,
            successfulAttempt.getHttpPort()); 
        parseAndAddReduceTaskCounters(reduceT,
            successfulAttempt.getCounters().toString());

        reduceTaskList.add(reduceT);

        // Add number of task attempts
        reduceT.setValue(ReduceTaskKeys.NUM_ATTEMPTS, 
            (new Integer(taskInfo.getAllTaskAttempts().size())).toString());

        // Add EXECUTION_TIME = FINISH_TIME - START_TIME
        long etime1 = reduceT.getLongValue(ReduceTaskKeys.FINISH_TIME) - 
        reduceT.getLongValue(ReduceTaskKeys.START_TIME);
        reduceT.setValue(ReduceTaskKeys.EXECUTION_TIME,
            (new Long(etime1)).toString());

      } else if (taskInfo.getTaskType().equals(TaskType.JOB_CLEANUP) ||
                 taskInfo.getTaskType().equals(TaskType.JOB_SETUP)) {
        //System.out.println("INFO: IGNORING TASK TYPE : "+task.get(Keys.TASK_TYPE));
      } else {
        System.err.println("UNKNOWN TASK TYPE : "+taskInfo.getTaskType());
      }
    }
  }
  
  /*
   * Get last successful task attempt to be added in the stats
   */
  private TaskAttemptInfo getLastSuccessfulTaskAttempt(TaskInfo task) {
    
    for (TaskAttemptInfo ai: task.getAllTaskAttempts().values()) {
      if (ai.getTaskStatus().equals(TaskStatus.State.SUCCEEDED.toString())) {
        return ai;
      }
    }
    return null;
  }
  
  /*
   * Popuate the job stats 
   */
  private void populate_Job (Hashtable<Enum, String> job, JobInfo jobInfo) throws ParseException {
    job.put(JobKeys.FINISH_TIME, String.valueOf(jobInfo.getFinishTime()));
    job.put(JobKeys.JOBID, jobInfo.getJobId().toString()); 
    job.put(JobKeys.JOBNAME, jobInfo.getJobname()); 
    job.put(JobKeys.USER, jobInfo.getUsername()); 
    job.put(JobKeys.JOBCONF, jobInfo.getJobConfPath()); 
    job.put(JobKeys.SUBMIT_TIME, String.valueOf(jobInfo.getSubmitTime())); 
    job.put(JobKeys.LAUNCH_TIME, String.valueOf(jobInfo.getLaunchTime())); 
    job.put(JobKeys.TOTAL_MAPS, String.valueOf(jobInfo.getTotalMaps())); 
    job.put(JobKeys.TOTAL_REDUCES, String.valueOf(jobInfo.getTotalReduces())); 
    job.put(JobKeys.FAILED_MAPS, String.valueOf(jobInfo.getFailedMaps())); 
    job.put(JobKeys.FAILED_REDUCES, String.valueOf(jobInfo.getFailedReduces())); 
    job.put(JobKeys.FINISHED_MAPS, String.valueOf(jobInfo.getFinishedMaps())); 
    job.put(JobKeys.FINISHED_REDUCES, 
        String.valueOf(jobInfo.getFinishedReduces())); 
    job.put(JobKeys.STATUS, jobInfo.getJobStatus().toString()); 
    job.put(JobKeys.JOB_PRIORITY, jobInfo.getPriority()); 
    parseAndAddJobCounters(job, jobInfo.getTotalCounters().toString());
  }
  
  
  /*
   * Parse and add the job counters
   */
  private void parseAndAddJobCounters(Hashtable<Enum, String> job, String counters) throws ParseException {
    Counters cnt = Counters.fromEscapedCompactString(counters);
    for (java.util.Iterator<Counters.Group> grps = cnt.iterator(); grps.hasNext(); ) {
      Counters.Group grp = grps.next();
      //String groupname = "<" + grp.getName() + ">::<" + grp.getDisplayName() + ">";
      for (java.util.Iterator<Counters.Counter> mycounters = grp.iterator(); mycounters.hasNext(); ) {
        Counters.Counter counter = mycounters.next();
        //String countername = "<"+counter.getName()+">::<"+counter.getDisplayName()+">::<"+counter.getValue()+">";
        //System.err.println("groupName:"+groupname+",countername: "+countername);
        String countername = grp.getDisplayName()+"."+counter.getDisplayName();
        String value = (new Long(counter.getValue())).toString();
        String[] parts = {countername,value};
        //System.err.println("part0:<"+parts[0]+">,:part1 <"+parts[1]+">");
        if (parts[0].equals("FileSystemCounters.FILE_BYTES_READ")) {
          job.put(JobKeys.FILE_BYTES_READ, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.FILE_BYTES_WRITTEN")) {
          job.put(JobKeys.FILE_BYTES_WRITTEN, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.HDFS_BYTES_READ")) {
          job.put(JobKeys.HDFS_BYTES_READ, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.HDFS_BYTES_WRITTEN")) {
          job.put(JobKeys.HDFS_BYTES_WRITTEN, parts[1]);
        } else if (parts[0].equals("Job Counters .Launched map tasks")) {
          job.put(JobKeys.LAUNCHED_MAPS, parts[1]);
        } else if (parts[0].equals("Job Counters .Launched reduce tasks")) {
          job.put(JobKeys.LAUNCHED_REDUCES, parts[1]);
        } else if (parts[0].equals("Job Counters .Data-local map tasks")) {
          job.put(JobKeys.DATALOCAL_MAPS, parts[1]);
        } else if (parts[0].equals("Job Counters .Rack-local map tasks")) {
          job.put(JobKeys.RACKLOCAL_MAPS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map input records")) {
          job.put(JobKeys.MAP_INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map output records")) {
          job.put(JobKeys.MAP_OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map input bytes")) {
          job.put(JobKeys.MAP_INPUT_BYTES, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map output bytes")) {
          job.put(JobKeys.MAP_OUTPUT_BYTES, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Combine input records")) {
          job.put(JobKeys.COMBINE_INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Combine output records")) {
          job.put(JobKeys.COMBINE_OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce input groups")) {
          job.put(JobKeys.REDUCE_INPUT_GROUPS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce input records")) {
          job.put(JobKeys.REDUCE_INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce output records")) {
          job.put(JobKeys.REDUCE_OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Spilled Records")) {
          job.put(JobKeys.SPILLED_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce shuffle bytes")) {
          job.put(JobKeys.SHUFFLE_BYTES, parts[1]);
        } else {
          System.err.println("JobCounterKey:<"+parts[0]+"> ==> NOT INCLUDED IN PERFORMANCE ADVISOR");
        }
      }
    }  
  }
  
  /*
   * Parse and add the Map task counters
   */
  private void parseAndAddMapTaskCounters(MapTaskStatistics mapTask, String counters) throws ParseException {
    Counters cnt = Counters.fromEscapedCompactString(counters);
    for (java.util.Iterator<Counters.Group> grps = cnt.iterator(); grps.hasNext(); ) {
      Counters.Group grp = grps.next();
      //String groupname = "<" + grp.getName() + ">::<" + grp.getDisplayName() + ">";
      for (java.util.Iterator<Counters.Counter> mycounters = grp.iterator(); mycounters.hasNext(); ) {
        Counters.Counter counter = mycounters.next();
        //String countername = "<"+counter.getName()+">::<"+counter.getDisplayName()+">::<"+counter.getValue()+">";
        //System.out.println("groupName:"+groupname+",countername: "+countername);
        String countername = grp.getDisplayName()+"."+counter.getDisplayName();
        String value = (new Long(counter.getValue())).toString();
        String[] parts = {countername,value};
        //System.out.println("part0:"+parts[0]+",:part1 "+parts[1]);
        if (parts[0].equals("FileSystemCounters.FILE_BYTES_READ")) {
          mapTask.setValue(MapTaskKeys.FILE_BYTES_READ, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.FILE_BYTES_WRITTEN")) {
          mapTask.setValue(MapTaskKeys.FILE_BYTES_WRITTEN, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.HDFS_BYTES_READ")) {
          mapTask.setValue(MapTaskKeys.HDFS_BYTES_READ, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.HDFS_BYTES_WRITTEN")) {
          mapTask.setValue(MapTaskKeys.HDFS_BYTES_WRITTEN, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map input records")) {
          mapTask.setValue(MapTaskKeys.INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map output records")) {
          mapTask.setValue(MapTaskKeys.OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Map output bytes")) {
          mapTask.setValue(MapTaskKeys.OUTPUT_BYTES, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Combine input records")) {
          mapTask.setValue(MapTaskKeys.COMBINE_INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Combine output records")) {
          mapTask.setValue(MapTaskKeys.COMBINE_OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Spilled Records")) {
          mapTask.setValue(MapTaskKeys.SPILLED_RECORDS, parts[1]);
        } else if (parts[0].equals("FileInputFormatCounters.BYTES_READ")) {
          mapTask.setValue(MapTaskKeys.INPUT_BYTES, parts[1]);
        } else {
          System.err.println("MapCounterKey:<"+parts[0]+"> ==> NOT INCLUDED IN PERFORMANCE ADVISOR MAP TASK");
        }
      }    
    }
  }
  
  /*
   * Parse and add the reduce task counters
   */
  private void parseAndAddReduceTaskCounters(ReduceTaskStatistics reduceTask, String counters) throws ParseException {
    Counters cnt = Counters.fromEscapedCompactString(counters);
    for (java.util.Iterator<Counters.Group> grps = cnt.iterator(); grps.hasNext(); ) {
      Counters.Group grp = grps.next();
      //String groupname = "<" + grp.getName() + ">::<" + grp.getDisplayName() + ">";
      for (java.util.Iterator<Counters.Counter> mycounters = grp.iterator(); mycounters.hasNext(); ) {
        Counters.Counter counter = mycounters.next();
        //String countername = "<"+counter.getName()+">::<"+counter.getDisplayName()+">::<"+counter.getValue()+">";
        //System.out.println("groupName:"+groupname+",countername: "+countername);
        String countername = grp.getDisplayName()+"."+counter.getDisplayName();
        String value = (new Long(counter.getValue())).toString();
        String[] parts = {countername,value};
        //System.out.println("part0:"+parts[0]+",:part1 "+parts[1]);
        if (parts[0].equals("FileSystemCounters.FILE_BYTES_READ")) {
          reduceTask.setValue(ReduceTaskKeys.FILE_BYTES_READ, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.FILE_BYTES_WRITTEN")) {
          reduceTask.setValue(ReduceTaskKeys.FILE_BYTES_WRITTEN, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.HDFS_BYTES_READ")) {
          reduceTask.setValue(ReduceTaskKeys.HDFS_BYTES_READ, parts[1]);
        } else if (parts[0].equals("FileSystemCounters.HDFS_BYTES_WRITTEN")) {
          reduceTask.setValue(ReduceTaskKeys.HDFS_BYTES_WRITTEN, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce input records")) {
          reduceTask.setValue(ReduceTaskKeys.INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce output records")) {
          reduceTask.setValue(ReduceTaskKeys.OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Combine input records")) {
          reduceTask.setValue(ReduceTaskKeys.COMBINE_INPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Combine output records")) {
          reduceTask.setValue(ReduceTaskKeys.COMBINE_OUTPUT_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce input groups")) {
          reduceTask.setValue(ReduceTaskKeys.INPUT_GROUPS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Spilled Records")) {
          reduceTask.setValue(ReduceTaskKeys.SPILLED_RECORDS, parts[1]);
        } else if (parts[0].equals("Map-Reduce Framework.Reduce shuffle bytes")) {
          reduceTask.setValue(ReduceTaskKeys.SHUFFLE_BYTES, parts[1]);
        } else {
          System.err.println("ReduceCounterKey:<"+parts[0]+"> ==> NOT INCLUDED IN PERFORMANCE ADVISOR REDUCE TASK");
        }
      }
    }    
  }
  
  /*
   * Print the Job Execution Statistics
   * TODO: split to pring job, map/reduce task list and individual map/reduce task stats
   */
  public void printJobExecutionStatistics() {
    /*
     * Print Job Counters
     */
    System.out.println("JOB COUNTERS *********************************************");
    int size = this._job.size();
    java.util.Iterator<Map.Entry<Enum, String>> kv = this._job.entrySet().iterator();
    for (int i = 0; i < size; i++)
    {
      Map.Entry<Enum, String> entry = (Map.Entry<Enum, String>) kv.next();
      Enum key = entry.getKey();
      String value = entry.getValue();
      System.out.println("Key:<" + key.name() + ">, value:<"+ value +">"); 
    }
    /*
     * 
     */
    System.out.println("MAP COUNTERS *********************************************");
    int size1 = this._mapTaskList.size();
    for (int i = 0; i < size1; i++)
    {
      System.out.println("MAP TASK *********************************************");
      this._mapTaskList.get(i).printKeys();
    }
    /*
     * 
     */
    System.out.println("REDUCE COUNTERS *********************************************");
    int size2 = this._mapTaskList.size();
    for (int i = 0; i < size2; i++)
    {
      System.out.println("REDUCE TASK *********************************************");
      this._reduceTaskList.get(i).printKeys();
    }
  }
  
  /*
   * Hash table keeping sorted lists of map tasks based on the specific map task key
   */
  private Hashtable <Enum, ArrayList<MapTaskStatistics>> _sortedMapTaskListsByKey = new Hashtable<Enum, ArrayList<MapTaskStatistics>>();
  
  /*
   * @return mapTaskList : ArrayList of MapTaskStatistics
   * @param mapTaskSortKey : Specific counter key used for sorting the task list
   * @param datatype : indicates the data type of the counter key used for sorting
   * If sort key is null then by default map tasks are sorted using map task ids.
   */
  public synchronized ArrayList<MapTaskStatistics> 
          getMapTaskList(Enum mapTaskSortKey, KeyDataType dataType) {
    
    /* 
     * If mapTaskSortKey is null then use the task id as a key.
     */
    if (mapTaskSortKey == null) {
      mapTaskSortKey = MapTaskKeys.TASK_ID;
    }
    
    if (this._sortedMapTaskListsByKey.get(mapTaskSortKey) == null) {
      ArrayList<MapTaskStatistics> newList = (ArrayList<MapTaskStatistics>)this._mapTaskList.clone();
      this._sortedMapTaskListsByKey.put(mapTaskSortKey, this.sortMapTasksByKey(newList, mapTaskSortKey, dataType));
    } 
    return this._sortedMapTaskListsByKey.get(mapTaskSortKey);
  }
  
  private ArrayList<MapTaskStatistics> sortMapTasksByKey (ArrayList<MapTaskStatistics> mapTasks, 
                         Enum key, Enum dataType) {
    MapCounterComparator mcc = new MapCounterComparator(key, dataType);
    Collections.sort (mapTasks, mcc);
    return mapTasks;
  }
  
  private class MapCounterComparator implements Comparator<MapTaskStatistics> {

    public Enum _sortKey;
    public Enum _dataType;
    
    public MapCounterComparator(Enum key, Enum dataType) {
      this._sortKey = key;
      this._dataType = dataType;
    }
    
    // Comparator interface requires defining compare method.
    public int compare(MapTaskStatistics a, MapTaskStatistics b) {
      if (this._dataType == KeyDataType.LONG) {
        long aa = a.getLongValue(this._sortKey);
        long bb = b.getLongValue(this._sortKey);
        if (aa<bb) return -1; if (aa==bb) return 0; if (aa>bb) return 1;
      } else {
        return a.getStringValue(this._sortKey).compareToIgnoreCase(b.getStringValue(this._sortKey));
      }
      
      return 0;
    }
  }
  
  /*
   * Reduce Array List sorting
   */
    private Hashtable <Enum, ArrayList<ReduceTaskStatistics>> _sortedReduceTaskListsByKey = new Hashtable<Enum,ArrayList<ReduceTaskStatistics>>();
  
    /*
     * @return reduceTaskList : ArrayList of ReduceTaskStatistics
   * @param reduceTaskSortKey : Specific counter key used for sorting the task list
   * @param dataType : indicates the data type of the counter key used for sorting
   * If sort key is null then, by default reduce tasks are sorted using task ids.
     */
  public synchronized ArrayList<ReduceTaskStatistics> 
                                getReduceTaskList (Enum reduceTaskSortKey, KeyDataType dataType) {
    
    /* 
     * If reduceTaskSortKey is null then use the task id as a key.
     */
    if (reduceTaskSortKey == null) {
      reduceTaskSortKey = ReduceTaskKeys.TASK_ID;
    }
    
    if (this._sortedReduceTaskListsByKey.get(reduceTaskSortKey) == null) {
      ArrayList<ReduceTaskStatistics> newList = (ArrayList<ReduceTaskStatistics>)this._reduceTaskList.clone();
      this._sortedReduceTaskListsByKey.put(reduceTaskSortKey, this.sortReduceTasksByKey(newList, reduceTaskSortKey, dataType));
    } 
    
    return this._sortedReduceTaskListsByKey.get(reduceTaskSortKey);  
  }
  
  private ArrayList<ReduceTaskStatistics> sortReduceTasksByKey (ArrayList<ReduceTaskStatistics> reduceTasks, 
                                Enum key, Enum dataType) {
    ReduceCounterComparator rcc = new ReduceCounterComparator(key, dataType);
    Collections.sort (reduceTasks, rcc);
    return reduceTasks;
  }
  
  private class ReduceCounterComparator implements Comparator<ReduceTaskStatistics> {

    public Enum _sortKey;
    public Enum _dataType;  //either long or string
    
    public ReduceCounterComparator(Enum key, Enum dataType) {
      this._sortKey = key;
      this._dataType = dataType;
    }
    
    // Comparator interface requires defining compare method.
    public int compare(ReduceTaskStatistics a, ReduceTaskStatistics b) {
      if (this._dataType == KeyDataType.LONG) {
        long aa = a.getLongValue(this._sortKey);
        long bb = b.getLongValue(this._sortKey);
        if (aa<bb) return -1; if (aa==bb) return 0; if (aa>bb) return 1;
      } else {
        return a.getStringValue(this._sortKey).compareToIgnoreCase(b.getStringValue(this._sortKey));
      }
      
      return 0;
    }
  }
}
