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
package org.tensorflow.bridge;

import org.tensorflow.distruntime.ClusterDef;
import org.tensorflow.distruntime.JobDef;

import java.util.*;

public class ClusterSpec {

  public ClusterDef cluster_def;
  public Map<String, Map<Integer, String>> cluster_spec;  // job_name task_index  address

  public ClusterSpec(Map<String, List<String>> cluster)  //cluster: job name --> address list    map
  {
    cluster_spec = new HashMap<String, Map<Integer, String>>();
    Iterator iter = cluster.entrySet().iterator();
    Integer i = 0;
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      String key = (String) entry.getKey();
      ArrayList<String> value = (ArrayList<String>) entry.getValue();
      Map<Integer, String> job_tasks = new HashMap<Integer, String>();
      i = 0;
      Iterator iter_address = value.iterator();
      while (iter_address.hasNext()) {
        job_tasks.put(i, (String) iter_address.next());
        i++;
      }
      cluster_spec.put(key, job_tasks);
    }
    this.make_cluster_def();
  }

  //Create a ClusterDef based on the given cluster_spec
  public void make_cluster_def() {
    Map<Integer, String> tasks;
    int taskIndex;
    String address;

    ClusterDef.Builder cluster_def_builder = ClusterDef.newBuilder();
    JobDef.Builder job_builder;
    JobDef job;

    Collection<String> jobSet = cluster_spec.keySet();
    List<String> jobList = new ArrayList<String>(jobSet);  //list就是一个job name的list
    Collections.sort(jobList);  //sort the key of cluster_spec

    for (int i = 0; i < jobList.size(); i++) {
      job_builder = JobDef.newBuilder();
      job_builder.setName(jobList.get(i));  //得到第i个job的name
      tasks = cluster_spec.get(jobList.get(i));  //第i个job对应的task的一个map, taskIndex-->address

      Collection<Integer> taskIndexSet = tasks.keySet();
      List<Integer> taskIndexList = new ArrayList<Integer>(taskIndexSet);
      Collections.sort(taskIndexList);  //sort the index of tasks
      for (int j = 0; j < taskIndexList.size(); j++) {
        taskIndex = taskIndexList.get(j);
        address = tasks.get(taskIndex);
        job_builder.putTasks(taskIndex, address);  //把taskIndex和对应的address放到job_builder里面
      }
      job = job_builder.build();
      cluster_def_builder.addJob(job);
    }

    cluster_def = cluster_def_builder.build();
  }

  //Judge whether the cluster is empty
  public boolean nonzero() {
    return cluster_def.isInitialized();
  }

  //Judge whether two cluster specs equal to each other
  public boolean equals(ClusterSpec other) {
    return cluster_def.equals(other.cluster_def);
  }

  //return a map from job names to their tasks(as the list form)
  public Map<String, List<String>> as_dict() {
    Map<String, List<String>> job_tasks_map = new HashMap<String, List<String>>();
    String job_name;
    List<String> jobs = this.jobs();
    for (int i = 0; i < jobs.size(); i++) {
      job_name = jobs.get(i);
      List<Integer> task_indices = this.task_indices(job_name);
      if (Collections.max(task_indices) + 1 == task_indices.size()) //the tasks indices are dense
      {
        job_tasks_map.put(job_name, this.job_tasks(job_name));
      } else //the tasks indices are not dense, manually make the list dense
      {
        List<String> tasks = new ArrayList<String>();
        Integer task_index;
        for (int j = 0; j < task_indices.size(); j++) {
          task_index = task_indices.get(j);
          tasks.add(this.task_address(job_name, task_index));

        }
      }
    }
    return job_tasks_map;
  }

  //返回所有的Job组成的list
  public List<String> jobs() {
    Collection<String> jobSet = cluster_spec.keySet();
    List<String> jobList = new ArrayList<String>(jobSet);
    return jobList;
  }

  //return the number of tasks defined in the given job
  public int num_tasks(String job_name) {
    return cluster_spec.get(job_name).keySet().size();
  }

  //return a list of valid task indices in the given job
  public List<Integer> task_indices(String job_name) {
    Collection<Integer> task_index_set = cluster_spec.get(job_name).keySet();
    List<Integer> task_index_list = new ArrayList<Integer>(task_index_set);
    return task_index_list;
  }

  //return the address of the given task in the given job
  public String task_address(String job_name, Integer task_index) {
    Map<Integer, String> job = cluster_spec.get(job_name);
    return job.get(task_index);
  }

  //return a list of tasks addresses, where the index in the list corresponds to the task index of each task
  public List<String> job_tasks(String job_name) {
    Map<Integer, String> job = cluster_spec.get(job_name);
    List<String> address_list = new ArrayList<String>(job.size() + 1);

    Collection<Integer> taskIndexSet = job.keySet();
    List<Integer> taskIndexList = new ArrayList<Integer>(taskIndexSet);
    Collections.sort(taskIndexList);  //sort the index of tasks
    int taskIndex;
    String address;
    for (int j = 0; j < taskIndexList.size(); j++) {
      taskIndex = taskIndexList.get(j);
      address = job.get(taskIndex);
      //address_list.set(taskIndex,address);
      address_list.add(address);
    }

    return address_list;
  }

  //Return the ClusterDef property
  public ClusterDef as_cluster_def() {
    return cluster_def;
  }
}