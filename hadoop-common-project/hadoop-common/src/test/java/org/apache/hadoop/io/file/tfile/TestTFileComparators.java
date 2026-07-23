/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.IOException;

import org.junit.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * 
 * Byte arrays test case class using GZ compression codec, base class of none
 * and LZO compression classes.
 * 
 */
public class TestTFileComparators {
  private static final String ROOT = GenericTestUtils.getTestDir().getAbsolutePath();
  private static final int BLOCK_SIZE = 512;
  private FileSystem fs;
  private Configuration conf;
  private Path path;
  private FSDataOutputStream out;
  private Writer writer;

  private static final String COMPRESSION = Compression.Algorithm.GZ.getName();
  private static final String OUTPUT_FILE = "TFileTestComparators";


  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    path = new Path(ROOT, OUTPUT_FILE);
    fs = path.getFileSystem(conf);
    out = fs.create(path);
  }

  @After
  public void tearDown() throws IOException {
    closeOutput();
    fs.delete(path, true);
  }

  // bad comparator format
  @Test
  public void testFailureBadComparatorNames() throws Exception {
    intercept(IllegalArgumentException.class, "Unsupported comparator", () ->
        new Writer(out, BLOCK_SIZE, COMPRESSION, "badcmp", conf));
  }

  // jclass that doesn't exist: fails to instantiate, not because the feature
  // is disabled.
  @Test
  public void testFailureBadJClassNames() throws Exception {
    conf.setBoolean(TFile.TFILE_COMPARATOR_JCLASS_ENABLED, true);
    intercept(IllegalArgumentException.class, "Failed to instantiate comparator",
        () -> new Writer(out, BLOCK_SIZE, COMPRESSION,
            "jclass: some.non.existence.clazz", conf));
  }

  // class exists but is not a RawComparator
  @Test
  public void testFailureBadJClasses() throws Exception {
    conf.setBoolean(TFile.TFILE_COMPARATOR_JCLASS_ENABLED, true);
    intercept(IllegalArgumentException.class, "Failed to instantiate comparator",
        () -> new Writer(out, BLOCK_SIZE, COMPRESSION,
            "jclass:org.apache.hadoop.io.file.tfile.Chunk", conf));
  }

  private void closeOutput() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (out != null) {
      out.close();
      out = null;
    }
  }
}
