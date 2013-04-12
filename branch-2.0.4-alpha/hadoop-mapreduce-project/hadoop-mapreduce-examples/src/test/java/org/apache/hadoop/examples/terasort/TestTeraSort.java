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
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Ignore;
@Ignore
public class TestTeraSort extends HadoopTestCase {
  
  public TestTeraSort()
      throws IOException {
    super(CLUSTER_MR, DFS_FS, 1, 1);
  }

  protected void tearDown() throws Exception {
    getFileSystem().delete(new Path(TEST_DIR), true);
    super.tearDown();
  }
  
  // Input/Output paths for sort
  private static final String TEST_DIR = 
    new File(System.getProperty("test.build.data", "/tmp"), "terasort")
    .getAbsolutePath();
  private static final Path SORT_INPUT_PATH = new Path(TEST_DIR, "sortin");
  private static final Path SORT_OUTPUT_PATH = new Path(TEST_DIR, "sortout");
  private static final Path TERA_OUTPUT_PATH = new Path(TEST_DIR, "validate");
  private static final String NUM_ROWS = "100"; 

  private void runTeraGen(Configuration conf, Path sortInput) 
      throws Exception {
    String[] genArgs = {NUM_ROWS, sortInput.toString()};
    
    // Run TeraGen
    assertEquals(ToolRunner.run(conf, new TeraGen(), genArgs), 0);
  }
  
  private void runTeraSort(Configuration conf,
      Path sortInput, Path sortOutput) throws Exception {

    // Setup command-line arguments to 'sort'
    String[] sortArgs = {sortInput.toString(), sortOutput.toString()};
    
    // Run Sort
    assertEquals(ToolRunner.run(conf, new TeraSort(), sortArgs), 0);
  }
  
  private void runTeraValidator(Configuration job, 
                                       Path sortOutput, Path valOutput) 
  throws Exception {
    String[] svArgs = {sortOutput.toString(), valOutput.toString()};

    // Run Tera-Validator
    assertEquals(ToolRunner.run(job, new TeraValidate(), svArgs), 0);
  }
  
  public void testTeraSort() throws Exception {
    // Run TeraGen to generate input for 'terasort'
    runTeraGen(createJobConf(), SORT_INPUT_PATH);

    // Run terasort
    runTeraSort(createJobConf(), SORT_INPUT_PATH, SORT_OUTPUT_PATH);

    // Run tera-validator to check if sort worked correctly
    runTeraValidator(createJobConf(), SORT_OUTPUT_PATH,
      TERA_OUTPUT_PATH);
  }

}
