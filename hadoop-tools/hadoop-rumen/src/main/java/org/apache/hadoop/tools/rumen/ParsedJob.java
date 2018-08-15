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
/**
 * 
 */
package org.apache.hadoop.tools.rumen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * This is a wrapper class around {@link LoggedJob}. This provides also the
 * extra information about the job obtained from job history which is not
 * written to the JSON trace file.
 */
public class ParsedJob extends LoggedJob {

  private static final Logger LOG = LoggerFactory.getLogger(ParsedJob.class);

  private Map<String, Long> totalCountersMap = new HashMap<String, Long>();
  private Map<String, Long> mapCountersMap = new HashMap<String, Long>();
  private Map<String, Long> reduceCountersMap = new HashMap<String, Long>();

  private String jobConfPath;
  private Map<JobACL, AccessControlList> jobAcls;

  ParsedJob() {

  }

  ParsedJob(String jobID) {
    super();

    setJobID(jobID);
  }

  /** Set the job total counters */
  void putTotalCounters(Map<String, Long> totalCounters) {
    this.totalCountersMap = totalCounters;
  }

  /**
   * @return the job total counters
   */
  public Map<String, Long> obtainTotalCounters() {
    return totalCountersMap;
  }

  /** Set the job level map tasks' counters */
  void putMapCounters(Map<String, Long> mapCounters) {
    this.mapCountersMap = mapCounters;
  }

  /**
   * @return the job level map tasks' counters
   */
  public Map<String, Long> obtainMapCounters() {
    return mapCountersMap;
  }

  /** Set the job level reduce tasks' counters */
  void putReduceCounters(Map<String, Long> reduceCounters) {
    this.reduceCountersMap = reduceCounters;
  }

  /**
   * @return the job level reduce tasks' counters
   */
  public Map<String, Long> obtainReduceCounters() {
    return reduceCountersMap;
  }

  /** Set the job conf path in staging dir on hdfs */
  void putJobConfPath(String confPath) {
    jobConfPath = confPath;
  }

  /**
   * @return the job conf path in staging dir on hdfs
   */
  public String obtainJobConfpath() {
    return jobConfPath;
  }

  /** Set the job acls */
  void putJobAcls(Map<JobACL, AccessControlList> acls) {
    jobAcls = acls;
  }

  /**
   * @return the job acls
   */
  public Map<JobACL, AccessControlList> obtainJobAcls() {
    return jobAcls;
  }

  /**
   * @return the list of map tasks of this job
   */
  public List<ParsedTask> obtainMapTasks() {
    List<LoggedTask> tasks = super.getMapTasks();
    return convertTasks(tasks);
  }

  /**
   * @return the list of reduce tasks of this job
   */
  public List<ParsedTask> obtainReduceTasks() {
    List<LoggedTask> tasks = super.getReduceTasks();
    return convertTasks(tasks);
  }

  /**
   * @return the list of other tasks of this job
   */
  public List<ParsedTask> obtainOtherTasks() {
    List<LoggedTask> tasks = super.getOtherTasks();
    return convertTasks(tasks);
  }

  /** As we know that this list of {@link LoggedTask} objects is actually a list
   * of {@link ParsedTask} objects, we go ahead and cast them.
   * @return the list of {@link ParsedTask} objects
   */
  private List<ParsedTask> convertTasks(List<LoggedTask> tasks) {
    List<ParsedTask> result = new ArrayList<ParsedTask>();

    for (LoggedTask t : tasks) {
      if (t instanceof ParsedTask) {
        result.add((ParsedTask)t);
      } else {
        throw new RuntimeException("Unexpected type of tasks in the list...");
      }
    }
    return result;
  }

  /** Dump the extra info of ParsedJob */
  void dumpParsedJob() {
    LOG.info("ParsedJob details:" + obtainTotalCounters() + ";"
        + obtainMapCounters() + ";" + obtainReduceCounters()
        + "\n" + obtainJobConfpath() + "\n" + obtainJobAcls()
        + ";Q=" + (getQueue() == null ? "null" : getQueue().getValue()));
    List<ParsedTask> maps = obtainMapTasks();
    for (ParsedTask task : maps) {
      task.dumpParsedTask();
    }
    List<ParsedTask> reduces = obtainReduceTasks();
    for (ParsedTask task : reduces) {
      task.dumpParsedTask();
    }
    List<ParsedTask> others = obtainOtherTasks();
    for (ParsedTask task : others) {
      task.dumpParsedTask();
    }
  }
}
