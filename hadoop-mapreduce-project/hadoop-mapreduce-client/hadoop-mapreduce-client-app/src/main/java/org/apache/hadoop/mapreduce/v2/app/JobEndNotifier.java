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
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
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
 * tries (0 would disable job end notification) and max time interval and a
 * proxy if needed</li><li>
 * The URL may contain sentinels which will be replaced by jobId and jobStatus 
 * (eg. SUCCEEDED/KILLED/FAILED) </li> </ul>
 */
public class JobEndNotifier implements Configurable {
  private static final String JOB_ID = "$jobId";
  private static final String JOB_STATUS = "$jobStatus";

  private Configuration conf;
  protected String userUrl;
  protected String proxyConf;
  protected int numTries; //Number of tries to attempt notification
  protected int waitInterval; //Time (ms) to wait between retrying notification
  protected int timeout; // Timeout (ms) on the connection and notification
  protected URL urlToNotify; //URL to notify read from the config
  protected Proxy proxyToUse = Proxy.NO_PROXY; //Proxy to use for notification

  /**
   * Parse the URL that needs to be notified of the end of the job, along
   * with the number of retries in case of failure, the amount of time to
   * wait between retries and proxy settings
   * @param conf the configuration 
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    
    numTries = Math.min(
      conf.getInt(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, 0) + 1
      , conf.getInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, 1)
    );
    waitInterval = Math.min(
    conf.getInt(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, 5000)
    , conf.getInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, 5000)
    );
    waitInterval = (waitInterval < 0) ? 5000 : waitInterval;

    timeout = conf.getInt(JobContext.MR_JOB_END_NOTIFICATION_TIMEOUT,
        JobContext.DEFAULT_MR_JOB_END_NOTIFICATION_TIMEOUT);

    userUrl = conf.get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL);

    proxyConf = conf.get(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY);

    //Configure the proxy to use if its set. It should be set like
    //proxyType@proxyHostname:port
    if(proxyConf != null && !proxyConf.equals("") &&
         proxyConf.lastIndexOf(":") != -1) {
      int typeIndex = proxyConf.indexOf("@");
      Proxy.Type proxyType = Proxy.Type.HTTP;
      if(typeIndex != -1 &&
        proxyConf.substring(0, typeIndex).compareToIgnoreCase("socks") == 0) {
        proxyType = Proxy.Type.SOCKS;
      }
      String hostname = proxyConf.substring(typeIndex + 1,
        proxyConf.lastIndexOf(":"));
      String portConf = proxyConf.substring(proxyConf.lastIndexOf(":") + 1);
      try {
        int port = Integer.parseInt(portConf);
        proxyToUse = new Proxy(proxyType,
          new InetSocketAddress(hostname, port));
        Log.info("Job end notification using proxy type \"" + proxyType + 
        "\" hostname \"" + hostname + "\" and port \"" + port + "\"");
      } catch(NumberFormatException nfe) {
        Log.warn("Job end notification couldn't parse configured proxy's port "
          + portConf + ". Not going to use a proxy");
      }
    }

  }

  public Configuration getConf() {
    return conf;
  }
  
  /**
   * Notify the URL just once. Use best effort.
   */
  protected boolean notifyURLOnce() {
    boolean success = false;
    try {
      Log.info("Job end notification trying " + urlToNotify);
      HttpURLConnection conn =
        (HttpURLConnection) urlToNotify.openConnection(proxyToUse);
      conn.setConnectTimeout(timeout);
      conn.setReadTimeout(timeout);
      conn.setAllowUserInteraction(false);
      if(conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        Log.warn("Job end notification to " + urlToNotify +" failed with code: "
        + conn.getResponseCode() + " and message \"" + conn.getResponseMessage()
        +"\"");
      }
      else {
        success = true;
        Log.info("Job end notification to " + urlToNotify + " succeeded");
      }
    } catch(IOException ioe) {
      Log.warn("Job end notification to " + urlToNotify + " failed", ioe);
    }
    return success;
  }

  /**
   * Notify a server of the completion of a submitted job. The user must have
   * configured MRJobConfig.MR_JOB_END_NOTIFICATION_URL
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
