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

import org.apache.hadoop.io.erasurecode.ECSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests write operation of DFS striped file with a random erasure code
 * policy except for the default policy under Datanode failure conditions.
 */
public class
    ParameterizedTestDFSStripedOutputStreamWithFailureWithRandomECPolicy extends
    ParameterizedTestDFSStripedOutputStreamWithFailure {

  private final ECSchema schema;

  private static final Logger LOG = LoggerFactory.getLogger(
      ParameterizedTestDFSStripedOutputStreamWithFailureWithRandomECPolicy
          .class.getName());

  public ParameterizedTestDFSStripedOutputStreamWithFailureWithRandomECPolicy(
      int base) {
    super(base);
    schema = StripedFileTestUtil.getRandomNonDefaultECPolicy().getSchema();
    LOG.info(schema.toString());
  }

  @Override
  public ECSchema getEcSchema() {
    return schema;
  }
}
