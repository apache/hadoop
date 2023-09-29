/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ExitUtil.ExitException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAggregateWordCount extends HadoopTestCase {
  public TestAggregateWordCount() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterEach
  public void tearDown() throws Exception {
    FileSystem fs = getFileSystem();
    if (fs != null) {
      fs.delete(TEST_DIR, true);
    }
    super.tearDown();
  }

  // Input/Output paths for sort
  private static final Path TEST_DIR = new Path(
      new File(System.getProperty("test.build.data", "/tmp"),
          "aggregatewordcount").getAbsoluteFile().toURI().toString());

  private static final Path INPUT_PATH = new Path(TEST_DIR, "inPath");
  private static final Path OUTPUT_PATH = new Path(TEST_DIR, "outPath");

  @Test
  void testAggregateTestCount()
      throws IOException, ClassNotFoundException, InterruptedException {

    ExitUtil.disableSystemExit();
    FileSystem fs = getFileSystem();
    fs.mkdirs(INPUT_PATH);
    Path file1 = new Path(INPUT_PATH, "file1");
    Path file2 = new Path(INPUT_PATH, "file2");
    FileUtil.write(fs, file1, "Hello World");
    FileUtil.write(fs, file2, "Hello Hadoop");

    String[] args =
        new String[]{INPUT_PATH.toString(), OUTPUT_PATH.toString(), "1",
            "textinputformat"};

    // Run AggregateWordCount Job.
    try {
      AggregateWordCount.main(args);
    } catch (ExitException e) {
      assertEquals(0, e.status);
    }

    String allEntries;
    try (FSDataInputStream stream = fs
        .open(new Path(OUTPUT_PATH, "part-r-00000"));) {
      allEntries = IOUtils.toString(stream, Charset.defaultCharset());
    }

    assertEquals("Hadoop\t1\n" + "Hello\t2\n" + "World\t1\n", allEntries);
  }
}
