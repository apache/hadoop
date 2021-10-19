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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;

import java.net.URL;

/**
 * An interface for implementing a custom Job end notifier. The built-in
 * Job end notifier uses a simple HTTP connection to notify the Job end status.
 * By implementing this interface and setting the
 * {@link MRJobConfig#MR_JOB_END_NOTIFICATION_CUSTOM_NOTIFIER_CLASS} property
 * in the map-reduce Job configuration you can have your own
 * notification mechanism. For now this still only works with HTTP/HTTPS URLs,
 * but by implementing this class you can choose how you want to make the
 * notification itself. For example you can choose to use a custom
 * HTTP library, or do a delegation token authentication, maybe set a
 * custom SSL context on the connection, etc. This means you still have to set
 * the {@link MRJobConfig#MR_JOB_END_NOTIFICATION_URL} property
 * in the Job's conf.
 */
public interface CustomJobEndNotifier {

  /**
   * The implementation should try to do a Job end notification only once.
   *
   * See {@link MRJobConfig#MR_JOB_END_RETRY_ATTEMPTS},
   * {@link MRJobConfig#MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS}
   * and org.apache.hadoop.mapreduce.v2.app.JobEndNotifier on how exactly
   * this method will be invoked.
   *
   * @param url the URL which needs to be notified
   *           (see {@link MRJobConfig#MR_JOB_END_NOTIFICATION_URL})
   * @param jobConf the map-reduce Job's configuration
   *
   * @return true if the notification was successful
   */
  boolean notifyOnce(URL url, Configuration jobConf) throws Exception;

}
