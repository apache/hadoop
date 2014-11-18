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

package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests if StreamXmlRecordReader will read the next record, _after_ the
 * end of a split if the split falls before the end of end-tag of a record.
 * Also tests if StreamXmlRecordReader will read a record twice if end of a
 * split is after few characters after the end-tag of a record but before the
 * begin-tag of next record.
 */
public class TestStreamXmlMultipleRecords extends TestStreaming
{
  private static final Log LOG = LogFactory.getLog(
      TestStreamXmlMultipleRecords.class);

  private boolean hasPerl = false;
  private long blockSize;
  private String isSlowMatch;

  // Our own configuration used for creating FileSystem object where
  // fs.local.block.size is set to 60 OR 80.
  // See 60th char in input. It is before the end of end-tag of a record.
  // See 80th char in input. It is in between the end-tag of a record and
  // the begin-tag of next record.
  private Configuration conf = null;

  private String myPerlMapper =
      "perl -n -a -e 'print join(qq(\\n), map { qq($_\\t1) } @F), qq(\\n);'";
  private String myPerlReducer =
      "perl -n -a -e '$freq{$F[0]}++; END { print qq(is\\t$freq{is}\\n); }'";

  public TestStreamXmlMultipleRecords() throws IOException {
    super();

    input = "<line>This is a single line,\nand it is containing multiple" +
        " words.</line>                     <line>Only is appears more than" +
        " once.</line>\n";
    outputExpect = "is\t3\n";

    map = myPerlMapper;
    reduce = myPerlReducer;

    hasPerl = UtilTest.hasPerlSupport();
  }

  @Override
  @Before
  public void setUp() throws IOException {
    super.setUp();
    // Without this closeAll() call, setting of FileSystem block size is
    // not effective and will be old block size set in earlier test.
    FileSystem.closeAll();
  }

  // Set file system block size such that split falls
  // (a) before the end of end-tag of a record (testStreamXmlMultiInner...) OR
  // (b) between records(testStreamXmlMultiOuter...)
  @Override
  protected Configuration getConf() {
    conf = new Configuration();
    conf.setLong("fs.local.block.size", blockSize);
    return conf;
  }

  @Override
  protected String[] genArgs() {
    args.add("-inputreader");
    args.add("StreamXmlRecordReader,begin=<line>,end=</line>,slowmatch=" +
        isSlowMatch);
    return super.genArgs();
  }

  /**
   * Tests if StreamXmlRecordReader will read the next record, _after_ the
   * end of a split if the split falls before the end of end-tag of a record.
   * Tests with slowmatch=false.
   * @throws Exception
   */
  @Test
  public void testStreamXmlMultiInnerFast() throws Exception {
    if (hasPerl) {
      blockSize = 60;

      isSlowMatch = "false";
      super.testCommandLine();
    }
    else {
      LOG.warn("No perl; skipping test.");
    }
  }

  /**
   * Tests if StreamXmlRecordReader will read a record twice if end of a
   * split is after few characters after the end-tag of a record but before the
   * begin-tag of next record.
   * Tests with slowmatch=false.
   * @throws Exception
   */
  @Test
  public void testStreamXmlMultiOuterFast() throws Exception {
    if (hasPerl) {
      blockSize = 80;

      isSlowMatch = "false";
      super.testCommandLine();
    }
    else {
      LOG.warn("No perl; skipping test.");
    }
  }

  /**
   * Tests if StreamXmlRecordReader will read the next record, _after_ the
   * end of a split if the split falls before the end of end-tag of a record.
   * Tests with slowmatch=true.
   * @throws Exception
   */
  @Test
  public void testStreamXmlMultiInnerSlow() throws Exception {
    if (hasPerl) {
      blockSize = 60;

      isSlowMatch = "true";
      super.testCommandLine();
    }
    else {
      LOG.warn("No perl; skipping test.");
    }
  }

  /**
   * Tests if StreamXmlRecordReader will read a record twice if end of a
   * split is after few characters after the end-tag of a record but before the
   * begin-tag of next record.
   * Tests with slowmatch=true.
   * @throws Exception
   */
  @Test
  public void testStreamXmlMultiOuterSlow() throws Exception {
    if (hasPerl) {
      blockSize = 80;

      isSlowMatch = "true";
      super.testCommandLine();
    }
    else {
      LOG.warn("No perl; skipping test.");
    }
  }

  @Override
  @Test
  public void testCommandLine() {
    // Do nothing
  }
}
