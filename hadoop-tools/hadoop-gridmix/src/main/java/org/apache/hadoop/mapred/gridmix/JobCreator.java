/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.gridmix;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum JobCreator {

  LOADJOB {
    @Override
    public GridmixJob createGridmixJob(
      Configuration gridmixConf, long submissionMillis, JobStory jobdesc,
      Path outRoot, UserGroupInformation ugi, int seq) throws IOException {

      // Build configuration for this simulated job
      Configuration conf = new Configuration(gridmixConf);
      dce.configureDistCacheFiles(conf, jobdesc.getJobConf());
      return new LoadJob(conf, submissionMillis, jobdesc, outRoot, ugi, seq);
    }

    @Override
    public boolean canEmulateDistCacheLoad() {
      return true;
    }
  },

  SLEEPJOB {
    private String[] hosts;
      
    @Override
    public GridmixJob createGridmixJob(
      Configuration conf, long submissionMillis, JobStory jobdesc, Path outRoot,
      UserGroupInformation ugi, int seq) throws IOException {
      int numLocations = conf.getInt(SLEEPJOB_RANDOM_LOCATIONS, 0);
      if (numLocations < 0) numLocations = 0;
      if (hosts == null) {
        final JobClient client = new JobClient(new JobConf(conf));
        ClusterStatus stat = client.getClusterStatus(true);
        final int nTrackers = stat.getTaskTrackers();
        final ArrayList<String> hostList = new ArrayList<String>(nTrackers);
        final Pattern trackerPattern = Pattern.compile("tracker_([^:]*):.*");
        final Matcher m = trackerPattern.matcher("");
        for (String tracker : stat.getActiveTrackerNames()) {
          m.reset(tracker);
          if (!m.find()) {
            continue;
          }
          final String name = m.group(1);
          hostList.add(name);
        }
        hosts = hostList.toArray(new String[hostList.size()]);
      }
      return new SleepJob(conf, submissionMillis, jobdesc, outRoot, ugi, seq,
          numLocations, hosts);
    }

    @Override
    public boolean canEmulateDistCacheLoad() {
      return false;
    }
  };

  public static final String GRIDMIX_JOB_TYPE = "gridmix.job.type";
  public static final String SLEEPJOB_RANDOM_LOCATIONS = 
    "gridmix.sleep.fake-locations";

  /**
   * Create Gridmix simulated job.
   * @param conf configuration of simulated job
   * @param submissionMillis At what time submission of this simulated job be
   *                         done
   * @param jobdesc JobStory obtained from trace
   * @param outRoot gridmix output directory
   * @param ugi UGI of job submitter of this simulated job
   * @param seq job sequence number
   * @return the created simulated job
   * @throws IOException
   */
  public abstract GridmixJob createGridmixJob(
    final Configuration conf, long submissionMillis, final JobStory jobdesc,
    Path outRoot, UserGroupInformation ugi, final int seq) throws IOException;

  public static JobCreator getPolicy(
    Configuration conf, JobCreator defaultPolicy) {
    return conf.getEnum(GRIDMIX_JOB_TYPE, defaultPolicy);
  }

  /**
   * @return true if gridmix simulated jobs of this job type can emulate
   *         distributed cache load
   */
  abstract boolean canEmulateDistCacheLoad();

  DistributedCacheEmulator dce;
  /**
   * This method is to be called before calling any other method in JobCreator
   * except canEmulateDistCacheLoad(), especially if canEmulateDistCacheLoad()
   * returns true for that job type.
   * @param e Distributed Cache Emulator
   */
  void setDistCacheEmulator(DistributedCacheEmulator e) {
    this.dce = e;
  }
}
