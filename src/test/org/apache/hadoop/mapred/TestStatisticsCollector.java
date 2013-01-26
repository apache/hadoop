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

import junit.framework.TestCase;

import org.apache.hadoop.mapred.StatisticsCollector.TimeWindow;
import org.apache.hadoop.mapred.StatisticsCollector.Stat;
import org.apache.hadoop.mapred.StatisticsCollector.Stat.TimeStat;

public class TestStatisticsCollector extends TestCase{

  public void testMovingWindow() throws Exception {
    StatisticsCollector collector = new StatisticsCollector(1);
    TimeWindow window = new TimeWindow("test", 6, 2);
    TimeWindow sincStart = StatisticsCollector.SINCE_START;
    TimeWindow[] windows = {sincStart, window};
    
    Stat stat = collector.createStat("m1", windows);
    
    stat.inc(3);
    collector.update();
    assertEquals(0, stat.getValues().get(window).getValue());
    assertEquals(3, stat.getValues().get(sincStart).getValue());
    
    stat.inc(3);
    collector.update();
    assertEquals((3+3), stat.getValues().get(window).getValue());
    assertEquals(6, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    assertEquals((3+3), stat.getValues().get(window).getValue());
    assertEquals(16, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    assertEquals((3+3+10+10), stat.getValues().get(window).getValue());
    assertEquals(26, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    stat.inc(10);
    collector.update();
    assertEquals((3+3+10+10+10+10), stat.getValues().get(window).getValue());
    assertEquals(46, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    assertEquals((3+3+10+10+10+10), stat.getValues().get(window).getValue());
    assertEquals(56, stat.getValues().get(sincStart).getValue());
    
    stat.inc(12);
    collector.update();
    assertEquals((10+10+10+10+10+12), stat.getValues().get(window).getValue());
    assertEquals(68, stat.getValues().get(sincStart).getValue());
    
    stat.inc(13);
    collector.update();
    assertEquals((10+10+10+10+10+12), stat.getValues().get(window).getValue());
    assertEquals(81, stat.getValues().get(sincStart).getValue());
    
    stat.inc(14);
    collector.update();
    assertEquals((10+10+10+12+13+14), stat.getValues().get(window).getValue());
    assertEquals(95, stat.getValues().get(sincStart).getValue());
  }

  public void testBucketing() throws Exception {
    StatisticsCollector collector = new StatisticsCollector();
    TimeWindow window = new TimeWindow("test", 33, 11);
    // We'll collect 3 buckets before we start removing: 33 / 11 = 3
    // We'll do 2 updates per bucket (5 is default period): 11 / 5 = 2
    TimeWindow[] windows = {window};
    Stat stat1 = collector.createStat("TaskTracker A", windows);

    // TT A starts out with 0 buckets
    assertEquals(0, stat1.getValues().get(window).getValue());
    stat1.inc(1);
    collector.update();
    assertEquals(0, stat1.getValues().get(window).getValue());
    stat1.inc(1);
    collector.update();
    assertEquals(2, stat1.getValues().get(window).getValue());
    stat1.inc(1);
    collector.update();
    assertEquals(2, stat1.getValues().get(window).getValue());
    stat1.inc(2);
    collector.update();
    assertEquals(2+3, stat1.getValues().get(window).getValue());
    stat1.inc(0);
    collector.update();
    assertEquals(2+3, stat1.getValues().get(window).getValue());
    stat1.inc(1);
    // At the next update, we now have 3 buckets for TT 1
    collector.update();
    assertEquals(2+3+1, stat1.getValues().get(window).getValue());
    stat1.inc(4);
    collector.update();
    assertEquals(2+3+1, stat1.getValues().get(window).getValue());
    // At the next update, we're going to drop the earliest bucket for TT A and
    // keep a max of 3 buckets forever
    collector.update();
    assertEquals(3+1+4, stat1.getValues().get(window).getValue());

    // A new TaskTracker connects and gets a Stat allocated for it
    Stat stat2 = collector.createStat("TaskTracker B", windows);

    // TT B starts out with 0 buckets even though TT A already has 3
    assertEquals(0, stat2.getValues().get(window).getValue());
    stat2.inc(10);
    collector.update();
    assertEquals(3+1+4, stat1.getValues().get(window).getValue());
    assertEquals(0, stat2.getValues().get(window).getValue());
    stat1.inc(3);
    stat2.inc(2);
    // At the next update, we're going to drop the earliest bucket for TT A
    // but we shouldn't drop the earliest bucket for TT B because it only
    // has one bucket so far (which would result in a value of 0 instead of 12)
    collector.update();
    assertEquals(1+4+3, stat1.getValues().get(window).getValue());
    assertEquals(12, stat2.getValues().get(window).getValue());
  }

}
