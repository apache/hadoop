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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MROutputFiles;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.CompressAwarePath;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
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


  @Test
  public void testEncryptedMerger() throws Throwable {
    jobConf.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    conf.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    TokenCache.setEncryptedSpillKey(new byte[16], credentials);
    UserGroupInformation.getCurrentUser().addCredentials(credentials);
    testInMemoryAndOnDiskMerger();
  }

  @Test
  public void testInMemoryAndOnDiskMerger() throws Throwable {
    JobID jobId = new JobID("a", 0);
    TaskAttemptID reduceId1 = new TaskAttemptID(
        new TaskID(jobId, TaskType.REDUCE, 0), 0);
    TaskAttemptID mapId1 = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 1), 0);
    TaskAttemptID mapId2 = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 2), 0);

    LocalDirAllocator lda = new LocalDirAllocator(MRConfig.LOCAL_DIR);

    MergeManagerImpl<Text, Text> mergeManager = new MergeManagerImpl<Text, Text>(
        reduceId1, jobConf, fs, lda, Reporter.NULL, null, null, null, null, null,
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
    List<InMemoryMapOutput<Text, Text>> mapOutputs1 =
        new ArrayList<InMemoryMapOutput<Text, Text>>();
    mapOutputs1.add(mapOutput1);
    mapOutputs1.add(mapOutput2);

    inMemoryMerger.merge(mapOutputs1);

    Assert.assertEquals(1, mergeManager.onDiskMapOutputs.size());

    TaskAttemptID reduceId2 = new TaskAttemptID(
        new TaskID(jobId, TaskType.REDUCE, 3), 0);
    TaskAttemptID mapId3 = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 4), 0);
    TaskAttemptID mapId4 = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 5), 0);
    // write map outputs
    Map<String, String> map3 = new TreeMap<String, String>();
    map3.put("apple", "awesome");
    map3.put("carrot", "amazing");
    Map<String, String> map4 = new TreeMap<String, String>();
    map4.put("banana", "bla");
    byte[] mapOutputBytes3 = writeMapOutput(conf, map3);
    byte[] mapOutputBytes4 = writeMapOutput(conf, map4);
    InMemoryMapOutput<Text, Text> mapOutput3 = new InMemoryMapOutput<Text, Text>(
        conf, mapId3, mergeManager, mapOutputBytes3.length, null, true);
    InMemoryMapOutput<Text, Text> mapOutput4 = new InMemoryMapOutput<Text, Text>(
        conf, mapId4, mergeManager, mapOutputBytes4.length, null, true);
    System.arraycopy(mapOutputBytes3, 0, mapOutput3.getMemory(), 0,
        mapOutputBytes3.length);
    System.arraycopy(mapOutputBytes4, 0, mapOutput4.getMemory(), 0,
        mapOutputBytes4.length);

//    // create merger and run merge
    MergeThread<InMemoryMapOutput<Text, Text>, Text, Text> inMemoryMerger2 =
        mergeManager.createInMemoryMerger();
    List<InMemoryMapOutput<Text, Text>> mapOutputs2 =
        new ArrayList<InMemoryMapOutput<Text, Text>>();
    mapOutputs2.add(mapOutput3);
    mapOutputs2.add(mapOutput4);

    inMemoryMerger2.merge(mapOutputs2);

    Assert.assertEquals(2, mergeManager.onDiskMapOutputs.size());

    List<CompressAwarePath> paths = new ArrayList<CompressAwarePath>();
    Iterator<CompressAwarePath> iterator = mergeManager.onDiskMapOutputs.iterator();
    List<String> keys = new ArrayList<String>();
    List<String> values = new ArrayList<String>();
    while (iterator.hasNext()) {
      CompressAwarePath next = iterator.next();
      readOnDiskMapOutput(conf, fs, next, keys, values);
      paths.add(next);
    }
    Assert.assertEquals(keys, Arrays.asList("apple", "banana", "carrot", "apple", "banana", "carrot"));
    Assert.assertEquals(values, Arrays.asList("awesome", "bla", "amazing", "disgusting", "pretty good", "delicious"));
    mergeManager.close();

    mergeManager = new MergeManagerImpl<Text, Text>(
        reduceId2, jobConf, fs, lda, Reporter.NULL, null, null, null, null, null,
        null, null, new Progress(), new MROutputFiles());

    MergeThread<CompressAwarePath,Text,Text> onDiskMerger = mergeManager.createOnDiskMerger();
    onDiskMerger.merge(paths);

    Assert.assertEquals(1, mergeManager.onDiskMapOutputs.size());

    keys = new ArrayList<String>();
    values = new ArrayList<String>();
    readOnDiskMapOutput(conf, fs, mergeManager.onDiskMapOutputs.iterator().next(), keys, values);
    Assert.assertEquals(keys, Arrays.asList("apple", "apple", "banana", "banana", "carrot", "carrot"));
    Assert.assertEquals(values, Arrays.asList("awesome", "disgusting", "pretty good", "bla", "amazing", "delicious"));

    mergeManager.close();
    Assert.assertEquals(0, mergeManager.inMemoryMapOutputs.size());
    Assert.assertEquals(0, mergeManager.inMemoryMergedMapOutputs.size());
    Assert.assertEquals(0, mergeManager.onDiskMapOutputs.size());
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
    FSDataInputStream in = CryptoUtils.wrapIfNecessary(conf, fs.open(path));

    IFile.Reader<Text, Text> reader = new IFile.Reader<Text, Text>(conf, in,
        fs.getFileStatus(path).getLen(), null, null);
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

  @SuppressWarnings( { "unchecked" })
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
    final float epsilon = 0.00001f;

    // Reading 6 keys total, 3 each in 2 segments, so each key read moves the
    // progress forward 1/6th of the way. Initially the first keys from each
    // segment have been read as part of the merge setup, so progress = 2/6.
    Assert.assertEquals(2/6.0f, mergeQueue.getProgress().get(), epsilon);

    // The first next() returns one of the keys already read during merge setup
    Assert.assertTrue(mergeQueue.next());
    Assert.assertEquals(2/6.0f, mergeQueue.getProgress().get(), epsilon);

    // Subsequent next() calls should read one key and move progress
    Assert.assertTrue(mergeQueue.next());
    Assert.assertEquals(3/6.0f, mergeQueue.getProgress().get(), epsilon);
    Assert.assertTrue(mergeQueue.next());
    Assert.assertEquals(4/6.0f, mergeQueue.getProgress().get(), epsilon);

    // At this point we've exhausted all of the keys in one segment
    // so getting the next key will return the already cached key from the
    // other segment
    Assert.assertTrue(mergeQueue.next());
    Assert.assertEquals(4/6.0f, mergeQueue.getProgress().get(), epsilon);

    // Subsequent next() calls should read one key and move progress
    Assert.assertTrue(mergeQueue.next());
    Assert.assertEquals(5/6.0f, mergeQueue.getProgress().get(), epsilon);
    Assert.assertTrue(mergeQueue.next());
    Assert.assertEquals(1.0f, mergeQueue.getProgress().get(), epsilon);

    // Now there should be no more input
    Assert.assertFalse(mergeQueue.next());
    Assert.assertEquals(1.0f, mergeQueue.getProgress().get(), epsilon);
    Assert.assertTrue(mergeQueue.getKey() == null);
    Assert.assertEquals(0, mergeQueue.getValue().getData().length);
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
    for (int i = 0; i < 2; i++) {
      segments.add(getUncompressedSegment(i));
    }
    return segments;
  }

  private List<Segment<Text, Text>> getCompressedSegments() throws IOException {
    List<Segment<Text, Text>> segments = new ArrayList<Segment<Text, Text>>();
    for (int i = 0; i < 2; i++) {
      segments.add(getCompressedSegment(i));
    }
    return segments;
  }

  private Segment<Text, Text> getUncompressedSegment(int i) throws IOException {
    return new Segment<Text, Text>(getReader(i, false), false);
  }

  private Segment<Text, Text> getCompressedSegment(int i) throws IOException {
    return new Segment<Text, Text>(getReader(i, true), false, 3000l);
  }

  @SuppressWarnings("unchecked")
  private Reader<Text, Text> getReader(int i, boolean isCompressedInput)
      throws IOException {
    Reader<Text, Text> readerMock = mock(Reader.class);
    when(readerMock.getLength()).thenReturn(30l);
    when(readerMock.getPosition()).thenReturn(0l).thenReturn(10l).thenReturn(
        20l);
    when(
        readerMock.nextRawKey(any(DataInputBuffer.class)))
        .thenAnswer(getKeyAnswer("Segment" + i, isCompressedInput));
    doAnswer(getValueAnswer("Segment" + i)).when(readerMock).nextRawValue(
        any(DataInputBuffer.class));

    return readerMock;
  }

  private Answer<?> getKeyAnswer(final String segmentName,
      final boolean isCompressedInput) {
    return new Answer<Object>() {
      int i = 0;

      @SuppressWarnings("unchecked")
      public Boolean answer(InvocationOnMock invocation) {
        if (i++ == 3) {
          return false;
        }
        Reader<Text,Text> mock = (Reader<Text,Text>) invocation.getMock();
        int multiplier = isCompressedInput ? 100 : 1;
        mock.bytesRead += 10 * multiplier;
        Object[] args = invocation.getArguments();
        DataInputBuffer key = (DataInputBuffer) args[0];
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
        key.reset(("Segment Value " + segmentName + i).getBytes(), 20);
        return null;
      }
    };
  }
}
