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
package org.apache.hadoop.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assume.assumeTrue;

/**
 * Test striped file write operation with data node failures with parameterized
 * test cases.
 */
@RunWith(Parameterized.class)
public class ParameterizedTestDFSStripedOutputStreamWithFailure extends
    TestDFSStripedOutputStreamWithFailureBase{
  public static final Logger LOG = LoggerFactory.getLogger(
      ParameterizedTestDFSStripedOutputStreamWithFailure.class);

  private int base;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> parameters = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      parameters.add(new Object[]{RANDOM.nextInt(220)});
    }
    return parameters;
  }

  public ParameterizedTestDFSStripedOutputStreamWithFailure(int base) {
    this.base = base;
  }

  @Test(timeout = 240000)
  public void runTestWithSingleFailure() {
    assumeTrue(base >= 0);
    if (base > lengths.size()) {
      base = base % lengths.size();
    }
    final int i = base;
    final Integer length = getLength(i);
    assumeTrue("Skip test " + i + " since length=null.", length != null);
    assumeTrue("Test " + i + ", length=" + length
        + ", is not chosen to run.", RANDOM.nextInt(16) != 0);
    System.out.println("Run test " + i + ", length=" + length);
    runTest(length);
  }
}
