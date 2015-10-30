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

package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptReportPBImpl;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos;
import org.apache.hadoop.yarn.util.Records;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskAttemptReport {

  @Test
  public void testSetRawCounters() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
  }

  @Test
  public void testBuildImplicitRawCounters() {
    TaskAttemptReportPBImpl report = new TaskAttemptReportPBImpl();
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    MRProtos.TaskAttemptReportProto protoVal = report.getProto();
    Counters counters = report.getCounters();
    assertTrue(protoVal.hasCounters());
  }

  @Test
  public void testCountersOverRawCounters() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    Counters altCounters = TypeConverter.toYarn(rCounters);
    report.setRawCounters(rCounters);
    report.setCounters(altCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    assertNotEquals(rCounters, altCounters);
    assertEquals(counters, altCounters);
  }

  @Test
  public void testUninitializedCounters() {
    // Create basic class
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    // Verify properties initialized to null
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetRawCountersToNull() {
    // Create basic class
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    // Set raw counters to null
    report.setRawCounters(null);
    // Verify properties still null
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());

  }

  @Test
  public void testSetCountersToNull() {
    // Create basic class
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    // Set raw counters to null
    report.setCounters(null);
    // Verify properties still null
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetNonNullCountersToNull() {
    // Create basic class
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    // Set raw counters
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    // Verify getCounters converts properly from raw to real
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    // Clear counters to null and then verify
    report.setCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetNonNullRawCountersToNull() {
    // Create basic class
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    // Set raw counters
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    // Verify getCounters converts properly from raw to real
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    // Clear counters to null and then verify
    report.setRawCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }
}

