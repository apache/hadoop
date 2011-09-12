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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.junit.Test;

public class TestCombineOutputCollector {
  private CombineOutputCollector<String, Integer> coc;

  @Test
  public void testCustomCollect() throws Throwable {
    //mock creation
    TaskReporter mockTaskReporter = mock(TaskReporter.class);
    Counters.Counter outCounter = new Counters.Counter();
    Writer<String, Integer> mockWriter = mock(Writer.class);

    Configuration conf = new Configuration();
    conf.set("mapred.combine.recordsBeforeProgress", "2");
    
    coc = new CombineOutputCollector<String, Integer>(outCounter, mockTaskReporter, conf);
    coc.setWriter(mockWriter);
    verify(mockTaskReporter, never()).progress();

    coc.collect("dummy", 1);
    verify(mockTaskReporter, never()).progress();
    
    coc.collect("dummy", 2);
    verify(mockTaskReporter, times(1)).progress();
  }
  
  @Test
  public void testDefaultCollect() throws Throwable {
    //mock creation
    TaskReporter mockTaskReporter = mock(TaskReporter.class);
    Counters.Counter outCounter = new Counters.Counter();
    Writer<String, Integer> mockWriter = mock(Writer.class);

    Configuration conf = new Configuration();
    
    coc = new CombineOutputCollector<String, Integer>(outCounter, mockTaskReporter, conf);
    coc.setWriter(mockWriter);
    verify(mockTaskReporter, never()).progress();

    for(int i = 0; i < Task.DEFAULT_MR_COMBINE_RECORDS_BEFORE_PROGRESS; i++) {
    	coc.collect("dummy", i);
    }
    verify(mockTaskReporter, times(1)).progress();
    for(int i = 0; i < Task.DEFAULT_MR_COMBINE_RECORDS_BEFORE_PROGRESS; i++) {
    	coc.collect("dummy", i);
    }
    verify(mockTaskReporter, times(2)).progress();
  }
}
