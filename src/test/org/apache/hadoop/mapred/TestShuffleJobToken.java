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


import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;

import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapreduce.security.JobTokens;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestShuffleJobToken {
  private static HttpServer server;
  private static URL baseUrl;
  private static File dir;
  private static final String JOB_ID = "job_20091117075357176_0001";
  
  // create fake url
  private URL getMapOutputURL(String host)  throws MalformedURLException {
    // Get the base url
    StringBuffer url = new StringBuffer(host);
    url.append("mapOutput?");
    url.append("job=" + JOB_ID + "&");
    url.append("reduce=0&");
    url.append("map=attempt");

    return new URL(url.toString());
  }

  @Before
  public void setUp() throws Exception {
    dir = new File(System.getProperty("build.webapps", "build/webapps") + "/test");
    System.out.println("dir="+dir.getAbsolutePath());
    if(!dir.exists()) {
      assertTrue(dir.mkdirs());
    }
    server = new HttpServer("test", "0.0.0.0", 0, true);
    server.addServlet("shuffle", "/mapOutput", TaskTracker.MapOutputServlet.class);
    server.start();
    int port = server.getPort();
    baseUrl = new URL("http://localhost:" + port + "/");
  }

  @After
  public void tearDown() throws Exception {
    if(dir.exists())
      dir.delete();
    if(server!=null)
      server.stop();
  }

  
  /**
   * try positive and negative case with invalid urlHash
   */
  @Test
  public void testInvalidJobToken()
  throws IOException, GeneralSecurityException {
    
    URL url = getMapOutputURL(baseUrl.toString());
    String enc_str = SecureShuffleUtils.buildMsgFrom(url);
    URLConnection connectionGood = url.openConnection();

    // create key 
    byte [] key= SecureShuffleUtils.getNewEncodedKey();
    
    // create fake TaskTracker - needed for keys storage
    JobTokens jt = new JobTokens();
    jt.setShuffleJobToken(key);
    TaskTracker tt  = new TaskTracker();
    addJobToken(tt, JOB_ID, jt); // fake id
    server.setAttribute("task.tracker", tt);

    // encode the url
    SecureShuffleUtils mac = new SecureShuffleUtils(key);
    String urlHashGood = mac.generateHash(enc_str.getBytes()); // valid hash
    
    // another the key
    byte [] badKey= SecureShuffleUtils.getNewEncodedKey();
    mac = new SecureShuffleUtils(badKey);
    String urlHashBad = mac.generateHash(enc_str.getBytes()); // invalid hash 
    
    // put url hash into http header
    connectionGood.addRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, urlHashGood);
    
    // valid url hash should not fail with security error
    try {
      connectionGood.getInputStream();
    } catch (IOException ie) {
      String msg = ie.getLocalizedMessage();
      if(msg.contains("Server returned HTTP response code: 401 for URL:")) {
        fail("securtity failure with valid urlHash:"+ie);
      }
      System.out.println("valid urlhash passed validation");
    } 
    // invalid url hash
    URLConnection connectionBad = url.openConnection();
    connectionBad.addRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, urlHashBad);
    
    try {
      connectionBad.getInputStream();
      fail("Connection should've failed because of invalid urlHash");
    } catch (IOException ie) {
      String msg = ie.getLocalizedMessage();
      if(!msg.contains("Server returned HTTP response code: 401 for URL:")) {
        fail("connection failed with other then validation error:"+ie);
      }
      System.out.println("validation worked, failed with:"+ie);
    } 
  }
  /*Note that this method is there for a unit testcase (TestShuffleJobToken)*/
  void addJobToken(TaskTracker tt, String jobIdStr, JobTokens jt) {
    JobID jobId = JobID.forName(jobIdStr);
    TaskTracker.RunningJob rJob = new TaskTracker.RunningJob(jobId);
    rJob.jobTokens = jt;
    synchronized (tt.runningJobs) {
      tt.runningJobs.put(jobId, rJob);
    }
  }

}
