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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestMerger {

  @Test
  public void testCompressed() throws IOException {
    testMergeShouldReturnProperProgress(getCompressedSegments());
  }
  
  @Test
  public void testUncompressed() throws IOException {
    testMergeShouldReturnProperProgress(getUncompressedSegments());
  }
  
  @SuppressWarnings( { "deprecation", "unchecked" })
  public void testMergeShouldReturnProperProgress(
      List<Segment<Text, Text>> segments) throws IOException {
    Configuration conf = new Configuration();
    JobConf jobConf = new JobConf();
    FileSystem fs = FileSystem.getLocal(conf);
    Path tmpDir = new Path("localpath");
    Class<Text> keyClass = (Class<Text>) jobConf.getMapOutputKeyClass();
    Class<Text> valueClass = (Class<Text>) jobConf.getMapOutputValueClass();
    RawComparator<Text> comparator = jobConf.getOutputKeyComparator();
    Counter readsCounter = new Counter();
    Counter writesCounter = new Counter();
    Progress mergePhase = new Progress();
    RawKeyValueIterator mergeQueue = Merger.merge(conf, fs, keyClass,
        valueClass, segments, 2, tmpDir, comparator, getReporter(),
        readsCounter, writesCounter, mergePhase);
    Assert.assertEquals(1.0f, mergeQueue.getProgress().get());
  }

  private Progressable getReporter() {
    Progressable reporter = new Progressable() {
      @Override
      public void progress() {
      }
    };
    return reporter;
  }

  private List<Segment<Text, Text>> getUncompressedSegments() throws IOException {
    List<Segment<Text, Text>> segments = new ArrayList<Segment<Text, Text>>();
    for (int i = 1; i < 1; i++) {
      segments.add(getUncompressedSegment(i));
      System.out.println("adding segment");
    }
    return segments;
  }
  
  private List<Segment<Text, Text>> getCompressedSegments() throws IOException {
    List<Segment<Text, Text>> segments = new ArrayList<Segment<Text, Text>>();
    for (int i = 1; i < 1; i++) {
      segments.add(getCompressedSegment(i));
      System.out.println("adding segment");
    }
    return segments;
  }
  
  private Segment<Text, Text> getUncompressedSegment(int i) throws IOException {
    return new Segment<Text, Text>(getReader(i), false);
  }

  private Segment<Text, Text> getCompressedSegment(int i) throws IOException {
    return new Segment<Text, Text>(getReader(i), false, 3000l);
  }

  @SuppressWarnings("unchecked")
  private Reader<Text, Text> getReader(int i) throws IOException {
    Reader<Text, Text> readerMock = mock(Reader.class);
    when(readerMock.getPosition()).thenReturn(0l).thenReturn(10l).thenReturn(
        20l);
    when(
        readerMock.nextRawKey(any(DataInputBuffer.class)))
        .thenAnswer(getKeyAnswer("Segment" + i));
    doAnswer(getValueAnswer("Segment" + i)).when(readerMock).nextRawValue(
        any(DataInputBuffer.class));

    return readerMock;
  }

  private Answer<?> getKeyAnswer(final String segmentName) {
    return new Answer<Object>() {
      int i = 0;

      public Boolean answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        DataInputBuffer key = (DataInputBuffer) args[0];
        if (i++ == 2) {
          return false;
        }
        key.reset(("Segement Key " + segmentName + i).getBytes(), 20);
        return true;
      }
    };
  }
  
  private Answer<?> getValueAnswer(final String segmentName) {
    return new Answer<Void>() {
      int i = 0;

      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        DataInputBuffer key = (DataInputBuffer) args[0];
        if (i++ == 2) {
          return null;
        }
        key.reset(("Segement Value " + segmentName + i).getBytes(), 20);
        return null;
      }
    };
  }
}