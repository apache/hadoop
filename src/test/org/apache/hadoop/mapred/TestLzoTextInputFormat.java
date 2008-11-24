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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.LzopCodec;
import org.apache.hadoop.mapred.LzoTextInputFormat.LzoIndex;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * Test the LzoTextInputFormat, make sure it splits the file properly and
 * returns the right data.
 */
public class TestLzoTextInputFormat extends TestCase {

  private static final Log LOG
    = LogFactory.getLog(TestLzoTextInputFormat.class.getName());
  
  private MessageDigest md5;
  private String lzoFileName = "file";
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    md5 = MessageDigest.getInstance("MD5");
  }
  
  /**
   * Make sure the lzo index class works as described.
   */
  public void testLzoIndex() {
    LzoIndex index = new LzoIndex();
    assertTrue(index.isEmpty());
    index = new LzoIndex(4);
    index.set(0, 0);
    index.set(1, 5);
    index.set(2, 10);
    index.set(3, 15);
    assertFalse(index.isEmpty());
    
    assertEquals(0, index.findNextPosition(-1));
    assertEquals(5, index.findNextPosition(1));
    assertEquals(5, index.findNextPosition(5));
    assertEquals(15, index.findNextPosition(11));
    assertEquals(15, index.findNextPosition(15));
    assertEquals(-1, index.findNextPosition(16));
  }
  
  /**
   * Index the file and make sure it splits properly.
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  public void testWithIndex() throws NoSuchAlgorithmException, IOException {
    runTest(true);
  }
  
  /**
   * Don't index the file and make sure it can be processed anyway.
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  public void testWithoutIndex() throws NoSuchAlgorithmException, IOException {
    runTest(false);
  }
  
  private void runTest(boolean testWithIndex) throws IOException, 
    NoSuchAlgorithmException {
    
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("Cannot run this test without the native lzo libraries");
      return;
    }

    String attempt = "attempt_200707121733_0001_m_000000_0";
    Path workDir = new Path(new Path(new Path(System.getProperty(
        "test.build.data", "."), "data"), FileOutputCommitter.TEMP_DIR_NAME),
        "_" + attempt);
    Path outputDir = workDir.getParent().getParent();

    JobConf conf = new JobConf();
    conf.set("mapred.task.id", attempt);
    conf.set("io.compression.codecs", LzopCodec.class.getName());

    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(workDir, true);
    localFs.mkdirs(workDir);
    FileInputFormat.setInputPaths(conf, outputDir);

    
    // create some input data
    byte[] expectedMd5 = createTestInput(outputDir, workDir, conf, localFs);

    if(testWithIndex) {
      Path lzoFile = new Path(workDir, lzoFileName);
      LzoTextInputFormat.createIndex(localFs, new Path(lzoFile
        + new LzopCodec().getDefaultExtension()));
    }
    
    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
    inputFormat.configure(conf);
    
    //it's left in the work dir
    FileInputFormat.setInputPaths(conf, workDir);

    int numSplits = 3;
    InputSplit[] is = inputFormat.getSplits(conf, numSplits);
    if(testWithIndex) {
      assertEquals(numSplits, is.length);
    } else {
      assertEquals(1, is.length);
    }

    for (InputSplit inputSplit : is) {
      RecordReader<LongWritable, Text> rr = inputFormat.getRecordReader(
          inputSplit, conf, Reporter.NULL);
      LongWritable key = rr.createKey();
      Text value = rr.createValue();

      while (rr.next(key, value)) {
        md5.update(value.getBytes(), 0, value.getLength());
      }

      rr.close();
    }

    assertTrue(Arrays.equals(expectedMd5, md5.digest()));
  }

  /**
   * Creates an lzo file with random data. 
   * 
   * @param outputDir Output directory
   * @param workDir Work directory, this is where the file is written to
   * @param fs File system we're using
   * @throws IOException
   */
  private byte[] createTestInput(Path outputDir, Path workDir, JobConf conf,
      FileSystem fs) throws IOException {

    RecordWriter<Text, Text> rw = null;
    
    md5.reset();
    
    try {
      TextOutputFormat<Text, Text> output = new TextOutputFormat<Text, Text>();
      TextOutputFormat.setCompressOutput(conf, true);
      TextOutputFormat.setOutputCompressorClass(conf, LzopCodec.class);
      TextOutputFormat.setOutputPath(conf, outputDir);
      TextOutputFormat.setWorkOutputPath(conf, workDir);

      rw = output.getRecordWriter(null, conf, lzoFileName, Reporter.NULL);

      //has to be enough data to create a couple of lzo blocks
      int charsToOutput = 10485760;
      char[] chars = "abcdefghijklmnopqrstuvwxyz\u00E5\u00E4\u00F6"
          .toCharArray();

      Random r = new Random(System.currentTimeMillis());
      Text key = new Text();
      Text value = new Text();
      int charsMax = chars.length - 1;
      for (int i = 0; i < charsToOutput;) {
        i += fillText(chars, r, charsMax, key);
        i += fillText(chars, r, charsMax, value);
        rw.write(key, value);
        md5.update(key.getBytes(), 0, key.getLength());
        //text output format writes tab between the key and value
        md5.update("\t".getBytes("UTF-8")); 
        md5.update(value.getBytes(), 0, value.getLength());
      }
    } finally {
      if (rw != null) {
        rw.close(Reporter.NULL);
      }
    }

    byte[] result = md5.digest();
    md5.reset();
    return result;
  }

  private int fillText(char[] chars, Random r, int charsMax, Text text) {
    StringBuilder sb = new StringBuilder();
    //get a reasonable string length 
    int stringLength = r.nextInt(charsMax * 2);
    for (int j = 0; j < stringLength; j++) {
      sb.append(chars[r.nextInt(charsMax)]);
    }
    text.set(sb.toString());
    return stringLength;
  }

}
