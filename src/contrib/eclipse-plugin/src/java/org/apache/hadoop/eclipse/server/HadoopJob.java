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

package org.apache.hadoop.eclipse.server;

/**
 * Helper class to pretty-print status for a hadoop job running on a MapReduce server.
 */

public class HadoopJob {
  String name;
  
  /**
   * Hadoop Job Id (useful to kill the job)
   */
  String jobId;

  boolean completed;

  String totalMaps;

  String totalReduces;

  String completedMaps;

  String completedReduces;

  String mapPercentage;

  String reducePercentage;

  private HadoopServer server;

  public HadoopJob(HadoopServer server) {
    this.server = server;
  }

  public void print() {
    System.out.println("Job name = " + name);
    System.out.println("Job id = " + jobId);
    System.out.println("Job total maps = " + totalMaps);
    System.out.println("Job completed maps = " + completedMaps);
    System.out.println("Map percentage complete = " + mapPercentage);
    System.out.println("Job total reduces = " + totalReduces);
    System.out.println("Job completed reduces = " + completedReduces);
    System.out.println("Reduce percentage complete = " + reducePercentage);
    System.out.flush();
  }

  public String getId() {
    return this.name;
  }
  
  public String getJobId() {
    return this.jobId;
  }
  
  public boolean isCompleted() {
    return this.completed;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof HadoopJob) && ((HadoopJob) o).name.equals(name);
  }

  public String getState() {
    return (!completed) ? "Running" : "Completed";
  }

  public String getStatus() {
    StringBuffer s = new StringBuffer();

    s.append("Maps : " + completedMaps + "/" + totalMaps);
    s.append(" (" + mapPercentage + ")");
    s.append("  Reduces : " + completedReduces + "/" + totalReduces);
    s.append(" (" + reducePercentage + ")");

    return s.toString();
  }

  public HadoopServer getServer() {
    return this.server;
  }
}
