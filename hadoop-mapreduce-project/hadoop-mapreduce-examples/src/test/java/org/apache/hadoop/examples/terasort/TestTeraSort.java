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
package org.apache.hadoop.examples.terasort;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestTeraSort extends HadoopTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestTeraSort.class);
  
  public TestTeraSort()
      throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }

  @After
  public void tearDown() throws Exception {
    getFileSystem().delete(TEST_DIR, true);
    super.tearDown();
  }
  
  // Input/Output paths for sort
  private static final Path TEST_DIR = new Path(new File(
    System.getProperty("test.build.data", "/tmp"), "terasort")
    .getAbsoluteFile().toURI().toString());
  private static final Path SORT_INPUT_PATH = new Path(TEST_DIR, "sortin");
  private static final Path SORT_OUTPUT_PATH = new Path(TEST_DIR, "sortout");
  private static final Path TERA_OUTPUT_PATH = new Path(TEST_DIR, "validate");
  private static final String NUM_ROWS = "100";

  private void runTeraGen(Configuration conf, Path sortInput)
      throws Exception {
    String[] genArgs = {NUM_ROWS, sortInput.toString()};

    // Run TeraGen
    assertEquals(0, ToolRunner.run(conf, new TeraGen(), genArgs));
  }

  private void runTeraSort(Configuration conf,
      Path sortInput, Path sortOutput) throws Exception {

    // Setup command-line arguments to 'sort'
    String[] sortArgs = {sortInput.toString(), sortOutput.toString()};

    // Run Sort
    assertEquals(0, ToolRunner.run(conf, new TeraSort(), sortArgs));
  }

  private void runTeraValidator(Configuration job,
                                       Path sortOutput, Path valOutput)
  throws Exception {
    String[] svArgs = {sortOutput.toString(), valOutput.toString()};

    // Run Tera-Validator
    assertEquals(0, ToolRunner.run(job, new TeraValidate(), svArgs));
  }

  @Test
  public void testTeraSort() throws Exception {
    // Run TeraGen to generate input for 'terasort'
    runTeraGen(createJobConf(), SORT_INPUT_PATH);

    // Run teragen again to check for FAE
    try {
      runTeraGen(createJobConf(), SORT_INPUT_PATH);
      fail("Teragen output overwritten!");
    } catch (FileAlreadyExistsException fae) {
      LOG.info("Expected exception: ", fae);
    }

    // Run terasort
    runTeraSort(createJobConf(), SORT_INPUT_PATH, SORT_OUTPUT_PATH);

    // Run terasort again to check for FAE
    try {
      runTeraSort(createJobConf(), SORT_INPUT_PATH, SORT_OUTPUT_PATH);
      fail("Terasort output overwritten!");
    } catch (FileAlreadyExistsException fae) {
      LOG.info("Expected exception: ", fae);
    }

    // Run tera-validator to check if sort worked correctly
    runTeraValidator(createJobConf(), SORT_OUTPUT_PATH,
      TERA_OUTPUT_PATH);
  }

  @Test
  public void testTeraSortWithLessThanTwoArgs() throws Exception {
    String[] args = new String[1];
    assertEquals(new TeraSort().run(args), 2);
  }

}
