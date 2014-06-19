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

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestLineRecordReader extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestLineRecordReader.class.getName());

  public void testStripBOM() throws IOException {
    LOG.info("testStripBOM");
    // the test data contains a BOM at the start of the file
    // confirm the BOM is skipped by LineRecordReader
    Path localCachePath = new Path(System.getProperty("test.cache.data"));
    Path txtPath = new Path(localCachePath, new Path("testBOM.txt"));
    String UTF8_BOM = "\uFEFF";
    LOG.info(txtPath.toString());
    File testFile = new File(txtPath.toString());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

    // read the data and check whether BOM is skipped
    FileSplit split = new FileSplit(txtPath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecords = 0;
    boolean firstLine = true;
    boolean skipBOM = true;
    String prevVal = null;
    while (reader.next(key, value)) {
      if (firstLine) {
        firstLine = false;
        if (value.toString().startsWith(UTF8_BOM)) {
          skipBOM = false;
        }
      } else {
        assertEquals("not same text", prevVal, value.toString());
      }
      prevVal = new String(value.toString());
      ++numRecords;
    }
    reader.close();

    assertTrue("BOM is not skipped", skipBOM);
  }

  public static void main(String[] args) throws Exception {
    new TestLineRecordReader().testStripBOM();
  }
}
