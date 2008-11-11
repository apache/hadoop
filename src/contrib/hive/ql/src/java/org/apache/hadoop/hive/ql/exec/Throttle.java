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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.net.URL;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.MalformedURLException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;

/*
 * Intelligence to make clients wait if the cluster is in a bad state.
 */
public class Throttle {

  // The percentage of maximum allocated memory that triggers GC
  // on job tracker. This could be overridden thru the jobconf.
  // The default is such that there is no throttling.
  static private int DEFAULT_MEMORY_GC_PERCENT = 100;

  // sleep this many seconds between each retry.
  // This could be overridden thru the jobconf.
  static private int DEFAULT_RETRY_PERIOD = 60;

  /**
   * fetch http://tracker.om:/gc.jsp?threshold=period
   */
  static void checkJobTracker(JobConf conf, Log LOG)  {

    try {
      byte buffer[] = new byte[1024]; 
      int threshold = conf.getInt("mapred.throttle.threshold.percent",
                                  DEFAULT_MEMORY_GC_PERCENT);
      int retry = conf.getInt("mapred.throttle.retry.period",
                              DEFAULT_RETRY_PERIOD);

      // If the threshold is 100 percent, then there is no throttling
      if (threshold == 100) {
        return;
      }

      // find the http port for the jobtracker
      String infoAddr = conf.get("mapred.job.tracker.http.address");
      if (infoAddr == null) {
        throw new IOException("Throttle: Unable to find job tracker info port.");
      }
      InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
      int infoPort = infoSocAddr.getPort();

      // This is the Job Tracker URL
      String tracker = "http://" +
                       JobTracker.getAddress(conf).getHostName() + ":" +
                       infoPort +
                       "/gc.jsp?threshold=" + threshold;

      while (true) {
        // read in the first 1K characters from the URL
        URL url = new URL(tracker);
        LOG.debug("Throttle: URL " + tracker);
        InputStream in = url.openStream();
        int numRead = in.read(buffer);
        in.close();
        String fetchString = new String(buffer);

        // fetch the xml tag <dogc>xxx</dogc>
        Pattern dowait = Pattern.compile("<dogc>",
                         Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        String[] results = dowait.split(fetchString);
        if (results.length != 2) {
          throw new IOException("Throttle: Unable to parse response of URL " + url + 
                                ". Get retuned " + fetchString);
        }
        dowait = Pattern.compile("</dogc>",
                         Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        results = dowait.split(results[1]);
        if (results.length < 1) {
          throw new IOException("Throttle: Unable to parse response of URL " + url + 
                                ". Get retuned " + fetchString);
        }

        // if the jobtracker signalled that the threshold is not exceeded, 
        // then we return immediately.
        if (results[0].trim().compareToIgnoreCase("false") == 0) {
          return;
        }

        // The JobTracker has exceeded its threshold and is doing a GC.
        // The client has to wait and retry.
        LOG.warn("Job is being throttled because of resource crunch on the " +
                 "JobTracker. Will retry in " + retry + " seconds..");
        Thread.sleep(retry * 1000L);
      }
    } catch (Exception e) {
      LOG.warn("Job is not being throttled. " + e);
    }
  }
}
