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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.mortbay.log.Log;

/**
 * <p>This class handles job end notification. Submitters of jobs can choose to
 * be notified of the end of a job by supplying a URL to which a connection
 * will be established.
 * <ul><li> The URL connection is fire and forget by default.</li> <li>
 * User can specify number of retry attempts and a time interval at which to
 * attempt retries</li><li>
 * Cluster administrators can set final parameters to set maximum number of
 * tries (0 would disable job end notification) and max time interval</li><li>
 * The URL may contain sentinels which will be replaced by jobId and jobStatus 
 * (eg. SUCCEEDED/KILLED/FAILED) </li> </ul>
 * </p>
 */
public class JobEndNotifier implements Configurable {
  final String JOB_ID = "$jobId";
  final String JOB_STATUS = "$jobStatus";

  private Configuration conf;
  protected String userUrl;
  protected int numTries; //Number of tries to attempt notification
  protected int waitInterval; //Time to wait between retrying notification
  protected URL urlToNotify; //URL to notify read from the config

  /**
   * Parse the URL that needs to be notified of the end of the job, along
   * with the number of retries in case of failure and the amount of time to
   * wait between retries
   * @param conf the configuration 
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    
    numTries = Math.min(
      conf.getInt(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, 0) + 1
      , conf.getInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, 1)
    );
    waitInterval = Math.min(
    conf.getInt(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, 5)
    , conf.getInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, 5)
    );
    waitInterval = (waitInterval < 0) ? 5 : waitInterval;

    userUrl = conf.get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL);
  }

  public Configuration getConf() {
    return conf;
  }
  
  /**
   * Notify the URL just once. Use best effort. Timeout hard coded to 5
   * seconds.
   */
  protected boolean notifyURLOnce() {
    boolean success = false;
    try {
      Log.info("Job end notification trying " + urlToNotify);
      URLConnection conn = urlToNotify.openConnection();
      conn.setConnectTimeout(5*1000);
      conn.setReadTimeout(5*1000);
      conn.setAllowUserInteraction(false);
      InputStream is = conn.getInputStream();
      conn.getContent();
      is.close();
      success = true;
      Log.info("Job end notification to " + urlToNotify + " succeeded");
    } catch(IOException ioe) {
      Log.warn("Job end notification to " + urlToNotify + " failed", ioe);
    }
    return success;
  }

  /**
   * Notify a server of the completion of a submitted job. The server must have
   * configured MRConfig.JOB_END_NOTIFICATION_URLS
   * @param jobReport JobReport used to read JobId and JobStatus
   * @throws InterruptedException
   */
  public void notify(JobReport jobReport)
    throws InterruptedException {
    // Do we need job-end notification?
    if (userUrl == null) {
      Log.info("Job end notification URL not set, skipping.");
      return;
    }

    //Do string replacements for jobId and jobStatus
    if (userUrl.contains(JOB_ID)) {
      userUrl = userUrl.replace(JOB_ID, jobReport.getJobId().toString());
    }
    if (userUrl.contains(JOB_STATUS)) {
      userUrl = userUrl.replace(JOB_STATUS, jobReport.getJobState().toString());
    }

    // Create the URL, ensure sanity
    try {
      urlToNotify = new URL(userUrl);
    } catch (MalformedURLException mue) {
      Log.warn("Job end notification couldn't parse " + userUrl, mue);
      return;
    }

    // Send notification
    boolean success = false;
    while (numTries-- > 0 && !success) {
      Log.info("Job end notification attempts left " + numTries);
      success = notifyURLOnce();
      if (!success) {
        Thread.sleep(waitInterval);
      }
    }
    if (!success) {
      Log.warn("Job end notification failed to notify : " + urlToNotify);
    } else {
      Log.info("Job end notification succeeded for " + jobReport.getJobId());
    }
  }
}
