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

import java.net.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests job end notification
 *
 */
public class TestJobEndNotifier extends JobEndNotifier {

  //Test maximum retries is capped by MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS
  private void testNumRetries(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "0");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, "10");
    setConf(conf);
    Assert.assertTrue("Expected numTries to be 0, but was " + numTries,
      numTries == 0 );

    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "1");
    setConf(conf);
    Assert.assertTrue("Expected numTries to be 1, but was " + numTries,
      numTries == 1 );

    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "20");
    setConf(conf);
    Assert.assertTrue("Expected numTries to be 11, but was " + numTries,
      numTries == 11 ); //11 because number of _retries_ is 10
  }

  //Test maximum retry interval is capped by
  //MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL
  private void testWaitInterval(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, "5000");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "1000");
    setConf(conf);
    Assert.assertTrue("Expected waitInterval to be 1000, but was "
      + waitInterval, waitInterval == 1000);

    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "10000");
    setConf(conf);
    Assert.assertTrue("Expected waitInterval to be 5000, but was "
      + waitInterval, waitInterval == 5000);

    //Test negative numbers are set to default
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "-10");
    setConf(conf);
    Assert.assertTrue("Expected waitInterval to be 5000, but was "
      + waitInterval, waitInterval == 5000);
  }

  private void testProxyConfiguration(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "somehost");
    setConf(conf);
    Assert.assertTrue("Proxy shouldn't be set because port wasn't specified",
      proxyToUse.type() == Proxy.Type.DIRECT);
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "somehost:someport");
    setConf(conf);
    Assert.assertTrue("Proxy shouldn't be set because port wasn't numeric",
      proxyToUse.type() == Proxy.Type.DIRECT);
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "somehost:1000");
    setConf(conf);
    Assert.assertTrue("Proxy should have been set but wasn't ",
      proxyToUse.toString().equals("HTTP @ somehost:1000"));
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "socks@somehost:1000");
    setConf(conf);
    Assert.assertTrue("Proxy should have been socks but wasn't ",
      proxyToUse.toString().equals("SOCKS @ somehost:1000"));
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "SOCKS@somehost:1000");
    setConf(conf);
    Assert.assertTrue("Proxy should have been socks but wasn't ",
      proxyToUse.toString().equals("SOCKS @ somehost:1000"));
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "sfafn@somehost:1000");
    setConf(conf);
    Assert.assertTrue("Proxy should have been http but wasn't ",
      proxyToUse.toString().equals("HTTP @ somehost:1000"));
    
  }

  /**
   * Test that setting parameters has the desired effect
   */
  @Test
  public void checkConfiguration() {
    Configuration conf = new Configuration();
    testNumRetries(conf);
    testWaitInterval(conf);
    testProxyConfiguration(conf);
  }

  protected int notificationCount = 0;
  @Override
  protected boolean notifyURLOnce() {
    boolean success = super.notifyURLOnce();
    notificationCount++;
    return success;
  }

  //Check retries happen as intended
  @Test
  public void testNotifyRetries() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_URL, "http://nonexistent");
    JobReport jobReport = Mockito.mock(JobReport.class);
 
    long startTime = System.currentTimeMillis();
    this.notificationCount = 0;
    this.setConf(conf);
    this.notify(jobReport);
    long endTime = System.currentTimeMillis();
    Assert.assertEquals("Only 1 try was expected but was : "
      + this.notificationCount, this.notificationCount, 1);
    Assert.assertTrue("Should have taken more than 5 seconds it took "
      + (endTime - startTime), endTime - startTime > 5000);

    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "3");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, "3");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "3000");
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, "3000");

    startTime = System.currentTimeMillis();
    this.notificationCount = 0;
    this.setConf(conf);
    this.notify(jobReport);
    endTime = System.currentTimeMillis();
    Assert.assertEquals("Only 3 retries were expected but was : "
      + this.notificationCount, this.notificationCount, 3);
    Assert.assertTrue("Should have taken more than 9 seconds it took "
      + (endTime - startTime), endTime - startTime > 9000);

  }

}
