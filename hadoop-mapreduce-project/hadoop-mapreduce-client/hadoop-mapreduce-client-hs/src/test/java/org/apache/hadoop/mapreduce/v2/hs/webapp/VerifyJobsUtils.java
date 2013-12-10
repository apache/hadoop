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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class VerifyJobsUtils {

  public static void verifyHsJobPartial(JSONObject info, Job job) throws JSONException {
    assertEquals("incorrect number of elements", 12, info.length());

    // everyone access fields
    verifyHsJobGeneric(job, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("state"),
        info.getString("queue"), info.getLong("startTime"),
        info.getLong("finishTime"), info.getInt("mapsTotal"),
        info.getInt("mapsCompleted"), info.getInt("reducesTotal"),
        info.getInt("reducesCompleted"));
  }
  
  public static void verifyHsJob(JSONObject info, Job job) throws JSONException {
    assertEquals("incorrect number of elements", 25, info.length());

    // everyone access fields
    verifyHsJobGeneric(job, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("state"),
        info.getString("queue"), info.getLong("startTime"),
        info.getLong("finishTime"), info.getInt("mapsTotal"),
        info.getInt("mapsCompleted"), info.getInt("reducesTotal"),
        info.getInt("reducesCompleted"));

    String diagnostics = "";
    if (info.has("diagnostics")) {
      diagnostics = info.getString("diagnostics");
    }

    // restricted access fields - if security and acls set
    verifyHsJobGenericSecure(job, info.getBoolean("uberized"), diagnostics,
        info.getLong("avgMapTime"), info.getLong("avgReduceTime"),
        info.getLong("avgShuffleTime"), info.getLong("avgMergeTime"),
        info.getInt("failedReduceAttempts"),
        info.getInt("killedReduceAttempts"),
        info.getInt("successfulReduceAttempts"),
        info.getInt("failedMapAttempts"), info.getInt("killedMapAttempts"),
        info.getInt("successfulMapAttempts"));

    // acls not being checked since
    // we are using mock job instead of CompletedJob
  }

  public static void verifyHsJobGeneric(Job job, String id, String user,
      String name, String state, String queue, long startTime, long finishTime,
      int mapsTotal, int mapsCompleted, int reducesTotal, int reducesCompleted) {
    JobReport report = job.getReport();

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(job.getID()),
        id);
    WebServicesTestUtils.checkStringMatch("user", job.getUserName().toString(),
        user);
    WebServicesTestUtils.checkStringMatch("name", job.getName(), name);
    WebServicesTestUtils.checkStringMatch("state", job.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("queue", job.getQueueName(), queue);

    assertEquals("startTime incorrect", report.getStartTime(), startTime);
    assertEquals("finishTime incorrect", report.getFinishTime(), finishTime);

    assertEquals("mapsTotal incorrect", job.getTotalMaps(), mapsTotal);
    assertEquals("mapsCompleted incorrect", job.getCompletedMaps(),
        mapsCompleted);
    assertEquals("reducesTotal incorrect", job.getTotalReduces(), reducesTotal);
    assertEquals("reducesCompleted incorrect", job.getCompletedReduces(),
        reducesCompleted);
  }

  public static void verifyHsJobGenericSecure(Job job, Boolean uberized,
      String diagnostics, long avgMapTime, long avgReduceTime,
      long avgShuffleTime, long avgMergeTime, int failedReduceAttempts,
      int killedReduceAttempts, int successfulReduceAttempts,
      int failedMapAttempts, int killedMapAttempts, int successfulMapAttempts) {

    String diagString = "";
    List<String> diagList = job.getDiagnostics();
    if (diagList != null && !diagList.isEmpty()) {
      StringBuffer b = new StringBuffer();
      for (String diag : diagList) {
        b.append(diag);
      }
      diagString = b.toString();
    }
    WebServicesTestUtils.checkStringMatch("diagnostics", diagString,
        diagnostics);

    assertEquals("isUber incorrect", job.isUber(), uberized);

    // unfortunately the following fields are all calculated in JobInfo
    // so not easily accessible without doing all the calculations again.
    // For now just make sure they are present.

    assertTrue("failedReduceAttempts not >= 0", failedReduceAttempts >= 0);
    assertTrue("killedReduceAttempts not >= 0", killedReduceAttempts >= 0);
    assertTrue("successfulReduceAttempts not >= 0",
        successfulReduceAttempts >= 0);

    assertTrue("failedMapAttempts not >= 0", failedMapAttempts >= 0);
    assertTrue("killedMapAttempts not >= 0", killedMapAttempts >= 0);
    assertTrue("successfulMapAttempts not >= 0", successfulMapAttempts >= 0);

    assertTrue("avgMapTime not >= 0", avgMapTime >= 0);
    assertTrue("avgReduceTime not >= 0", avgReduceTime >= 0);
    assertTrue("avgShuffleTime not >= 0", avgShuffleTime >= 0);
    assertTrue("avgMergeTime not >= 0", avgMergeTime >= 0);

  }

}
