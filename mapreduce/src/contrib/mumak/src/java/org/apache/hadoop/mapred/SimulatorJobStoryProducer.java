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
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;
import org.apache.hadoop.tools.rumen.ZombieCluster;
import org.apache.hadoop.tools.rumen.ZombieJob;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;

/**
 * This class creates {@link JobStory} objects from trace file in rumen format.
 * It is a proxy class over {@link ZombieJobProducer}, and adjusts the
 * submission time to be aligned with simulation time.
 */
public class SimulatorJobStoryProducer implements JobStoryProducer {
  private final ZombieJobProducer producer;
  private final long firstJobStartTime;
  private long relativeTime = -1;
  private boolean firstJob = true;

  public SimulatorJobStoryProducer(Path path, ZombieCluster cluster,
      long firstJobStartTime, Configuration conf) throws IOException {
    this(path, cluster, firstJobStartTime, conf, System.nanoTime());
  }

  public SimulatorJobStoryProducer(Path path, ZombieCluster cluster,
      long firstJobStartTime, Configuration conf, long seed) throws IOException {
    producer = new ZombieJobProducer(path, cluster, conf, seed);
    this.firstJobStartTime = firstJobStartTime;
  }

  /**
   * Filter some jobs being fed to the simulator. For now, we filter out killed
   * jobs to facilitate debugging.
   * 
   * @throws IOException
   */
  private JobStory getNextJobFiltered() throws IOException {
    while (true) {
      ZombieJob job = producer.getNextJob();
      if (job == null) {
        return null;
      }
      if (job.getOutcome() == Pre21JobHistoryConstants.Values.KILLED) {
        continue;
      }
      if (job.getNumberMaps() == 0) {
        continue;
      }
      if (job.getNumLoggedMaps() == 0) {
        continue;
      }
      return job;
    }
  }

  @Override
  public JobStory getNextJob() throws IOException {
    JobStory job = getNextJobFiltered();
    if (job == null)
      return null;
    if (firstJob) {
      firstJob = false;
      relativeTime = job.getSubmissionTime() - firstJobStartTime;
    }

    return new SimulatorJobStory(job, job.getSubmissionTime() - relativeTime);
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }
}
