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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class VerifyJobsUtils {

  public static void verifyHsJobPartial(JSONObject info, Job job) throws JSONException {
    assertEquals(12, info.length(), "incorrect number of elements");

    // everyone access fields
    verifyHsJobGeneric(job, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("state"),
        info.getString("queue"), info.getLong("startTime"),
        info.getLong("finishTime"), info.getInt("mapsTotal"),
        info.getInt("mapsCompleted"), info.getInt("reducesTotal"),
        info.getInt("reducesCompleted"));
  }
  
  public static void verifyHsJob(JSONObject info, Job job) throws JSONException {
    assertEquals(25, info.length(), "incorrect number of elements");

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
    WebServicesTestUtils.checkStringMatch("user", job.getUserName(),
        user);
    WebServicesTestUtils.checkStringMatch("name", job.getName(), name);
    WebServicesTestUtils.checkStringMatch("state", job.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("queue", job.getQueueName(), queue);

    assertEquals(report.getStartTime(), startTime, "startTime incorrect");
    assertEquals(report.getFinishTime(), finishTime, "finishTime incorrect");

    assertEquals(job.getTotalMaps(), mapsTotal, "mapsTotal incorrect");
    assertEquals(job.getCompletedMaps(),
        mapsCompleted, "mapsCompleted incorrect");
    assertEquals(job.getTotalReduces(), reducesTotal, "reducesTotal incorrect");
    assertEquals(job.getCompletedReduces(),
        reducesCompleted, "reducesCompleted incorrect");
  }

  public static void verifyHsJobGenericSecure(Job job, Boolean uberized,
      String diagnostics, long avgMapTime, long avgReduceTime,
      long avgShuffleTime, long avgMergeTime, int failedReduceAttempts,
      int killedReduceAttempts, int successfulReduceAttempts,
      int failedMapAttempts, int killedMapAttempts, int successfulMapAttempts) {

    String diagString = "";
    List<String> diagList = job.getDiagnostics();
    if (diagList != null && !diagList.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for (String diag : diagList) {
        b.append(diag);
      }
      diagString = b.toString();
    }
    WebServicesTestUtils.checkStringMatch("diagnostics", diagString,
        diagnostics);

    assertEquals(job.isUber(), uberized, "isUber incorrect");

    // unfortunately the following fields are all calculated in JobInfo
    // so not easily accessible without doing all the calculations again.
    // For now just make sure they are present.

    assertTrue(failedReduceAttempts >= 0, "failedReduceAttempts not >= 0");
    assertTrue(killedReduceAttempts >= 0, "killedReduceAttempts not >= 0");
    assertTrue(successfulReduceAttempts >= 0,
        "successfulReduceAttempts not >= 0");

    assertTrue(failedMapAttempts >= 0, "failedMapAttempts not >= 0");
    assertTrue(killedMapAttempts >= 0, "killedMapAttempts not >= 0");
    assertTrue(successfulMapAttempts >= 0, "successfulMapAttempts not >= 0");

    assertTrue(avgMapTime >= 0, "avgMapTime not >= 0");
    assertTrue(avgReduceTime >= 0, "avgReduceTime not >= 0");
    assertTrue(avgShuffleTime >= 0, "avgShuffleTime not >= 0");
    assertTrue(avgMergeTime >= 0, "avgMergeTime not >= 0");

  }

}
