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
package org.apache.hadoop.mapred.gridmix.test.system;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;
import org.apache.hadoop.tools.rumen.ZombieJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Build the job stories with a given trace file. 
 */
public class GridmixJobStory {
  private static Log LOG = LogFactory.getLog(GridmixJobStory.class);
  private Path path;
  private Map<JobID, ZombieJob> zombieJobs;
  private Configuration conf;
  
  public GridmixJobStory(Path path, Configuration conf) {
    this.path = path;
    this.conf = conf;
    try {
       zombieJobs = buildJobStories();
       if(zombieJobs == null) {
          throw new NullPointerException("No jobs found in a " 
              + " given trace file.");
       }
    } catch (IOException ioe) {
      LOG.warn("Error:" + ioe.getMessage());
    } catch (NullPointerException npe) {
      LOG.warn("Error:" + npe.getMessage());
    }
  }
  
  /**
   * Get the zombie jobs as a map.
   * @return the zombie jobs map.
   */
  public Map<JobID, ZombieJob> getZombieJobs() {
    return zombieJobs;
  }
  
  /**
   * Get the zombie job of a given job id.
   * @param jobId - gridmix job id.
   * @return - the zombie job object.
   */
  public ZombieJob getZombieJob(JobID jobId) {
    return zombieJobs.get(jobId);
  }
  
  private Map<JobID, ZombieJob> buildJobStories() throws IOException {
    ZombieJobProducer zjp = new ZombieJobProducer(path,null, conf);
    Map<JobID, ZombieJob> hm = new HashMap<JobID, ZombieJob>();
    ZombieJob zj = zjp.getNextJob();
    while (zj != null) {
      hm.put(zj.getJobID(),zj);
      zj = zjp.getNextJob();
    }
    if (hm.size() == 0) {
      return null;
    } else {
      return hm;
    }
  }
}
