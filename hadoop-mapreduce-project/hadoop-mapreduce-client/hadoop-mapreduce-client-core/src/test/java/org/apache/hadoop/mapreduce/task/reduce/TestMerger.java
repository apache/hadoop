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
package org.apache.hadoop.mapreduce.task.reduce;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MROutputFiles;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestMerger {

  private Configuration conf;
  private JobConf jobConf;
  private FileSystem fs;
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    jobConf = new JobConf();
    fs = FileSystem.getLocal(conf);
  }
  
  @After
  public void cleanup() throws IOException {    
    fs.delete(new Path(jobConf.getLocalDirs()[0]), true);
  }
  
  @Test
  public void testInMemoryMerger() throws IOException {
    JobID jobId = new JobID("a", 0);
    TaskAttemptID reduceId = new TaskAttemptID(
        new TaskID(jobId, TaskType.REDUCE, 0), 0);
    TaskAttemptID mapId1 = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 1), 0);
    TaskAttemptID mapId2 = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 2), 0);
    
    LocalDirAllocator lda = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    
    MergeManagerImpl<Text, Text> mergeManager = new MergeManagerImpl<Text, Text>(
        reduceId, jobConf, fs, lda, Reporter.NULL, null, null, null, null, null,
        null, null, new Progress(), new MROutputFiles());
    
    // write map outputs
    Map<String, String> map1 = new TreeMap<String, String>();
    map1.put("apple", "disgusting");
    map1.put("carrot", "delicious");
    Map<String, String> map2 = new TreeMap<String, String>();
    map1.put("banana", "pretty good");
    byte[] mapOutputBytes1 = writeMapOutput(conf, map1);
    byte[] mapOutputBytes2 = writeMapOutput(conf, map2);
    InMemoryMapOutput<Text, Text> mapOutput1 = new InMemoryMapOutput<Text, Text>(
        conf, mapId1, mergeManager, mapOutputBytes1.length, null, true);
    InMemoryMapOutput<Text, Text> mapOutput2 = new InMemoryMapOutput<Text, Text>(
        conf, mapId2, mergeManager, mapOutputBytes2.length, null, true);
    System.arraycopy(mapOutputBytes1, 0, mapOutput1.getMemory(), 0,
        mapOutputBytes1.length);
    System.arraycopy(mapOutputBytes2, 0, mapOutput2.getMemory(), 0,
        mapOutputBytes2.length);
    
    // create merger and run merge
    MergeThread<InMemoryMapOutput<Text, Text>, Text, Text> inMemoryMerger =
        mergeManager.createInMemoryMerger();
    List<InMemoryMapOutput<Text, Text>> mapOutputs =
        new ArrayList<InMemoryMapOutput<Text, Text>>();
    mapOutputs.add(mapOutput1);
    mapOutputs.add(mapOutput2);
    
    inMemoryMerger.merge(mapOutputs);
    
    Assert.assertEquals(1, mergeManager.onDiskMapOutputs.size());
    Path outPath = mergeManager.onDiskMapOutputs.iterator().next();
    
    List<String> keys = new ArrayList<String>();
    List<String> values = new ArrayList<String>();
    readOnDiskMapOutput(conf, fs, outPath, keys, values);
    Assert.assertEquals(keys, Arrays.asList("apple", "banana", "carrot"));
    Assert.assertEquals(values, Arrays.asList("disgusting", "pretty good", "delicious"));
  }
  
  private byte[] writeMapOutput(Configuration conf, Map<String, String> keysToValues)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer<Text, Text> writer = new IFile.Writer<Text, Text>(conf, fsdos,
        Text.class, Text.class, null, null);
    for (String key : keysToValues.keySet()) {
      String value = keysToValues.get(key);
      writer.append(new Text(key), new Text(value));
    }
    writer.close();
    return baos.toByteArray();
  }
  
  private void readOnDiskMapOutput(Configuration conf, FileSystem fs, Path path,
      List<String> keys, List<String> values) throws IOException {
    IFile.Reader<Text, Text> reader = new IFile.Reader<Text, Text>(conf, fs,
        path, null, null);
    DataInputBuffer keyBuff = new DataInputBuffer();
    DataInputBuffer valueBuff = new DataInputBuffer();
    Text key = new Text();
    Text value = new Text();
    while (reader.nextRawKey(keyBuff)) {
      key.readFields(keyBuff);
      keys.add(key.toString());
      reader.nextRawValue(valueBuff);
      value.readFields(valueBuff);
      values.add(value.toString());
    }
  }
  
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
    }
    return segments;
  }
  
  private List<Segment<Text, Text>> getCompressedSegments() throws IOException {
    List<Segment<Text, Text>> segments = new ArrayList<Segment<Text, Text>>();
    for (int i = 1; i < 1; i++) {
      segments.add(getCompressedSegment(i));
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
        key.reset(("Segment Key " + segmentName + i).getBytes(), 20);
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
        key.reset(("Segment Value " + segmentName + i).getBytes(), 20);
        return null;
      }
    };
  }
}