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

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

/**
 * This class tests if hadoopStreaming returns Exception 
 * on failure when submitted an invalid/failed job
 * The test case provides an invalid input file for map/reduce job as
 * a unit test case
 */
public class TestStreamingFailure extends TestStreaming
{

  protected File INVALID_INPUT_FILE;

  public TestStreamingFailure() throws IOException
  {
    INVALID_INPUT_FILE = new File("invalid_input.txt");
  }

  @Override
  protected void setInputOutput() {
    inputFile = INVALID_INPUT_FILE.getAbsolutePath();
    outDir = OUTPUT_DIR.getAbsolutePath();
  }

  @Override
  @Test
  public void testCommandLine() throws IOException {
    int returnStatus = runStreamJob();
    assertEquals("Streaming Job Failure code expected", 5, returnStatus);
  }
}
