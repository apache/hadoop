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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;


public class MapReduceTrackingUriPlugin extends TrackingUriPlugin implements
    Configurable {

  @Override
  public void setConf(Configuration conf) {
    Configuration jobConf = null;
    // Force loading of mapred configuration.
    if (conf != null) {
      jobConf = new JobConf(conf);
    } else {
      jobConf = new JobConf();
    }
    super.setConf(jobConf);
  }

  /**
   * Gets the URI to access the given application on MapReduce history server
   * @param id the ID for which a URI is returned
   * @return the tracking URI
   * @throws URISyntaxException
   */
  @Override
  public URI getTrackingUri(ApplicationId id) throws URISyntaxException {
    String jobSuffix = id.toString().replaceFirst("^application_", "job_");
    String historyServerAddress =
        MRWebAppUtil.getJHSWebappURLWithScheme(getConf());
    return new URI(historyServerAddress + "/jobhistory/job/"+ jobSuffix);
  }
}
