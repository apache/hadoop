/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.audit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.audit.AuditTool.AUDIT;

/**
 * This will implement tests on AuditTool class.
 */
public class TestAuditTool extends AbstractAuditToolTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAuditTool.class);

  /**
   * Testing run method in AuditTool class by passing source and destination
   * paths.
   */
  @Test
  public void testRun() throws Exception {
    Path s3LogsPath = new Path("s3a://sravani-data/logs2");
    Path s3DestPath = new Path("s3a://sravani-data/summary");
    run(AUDIT, s3DestPath, s3LogsPath);
    S3AFileSystem s3AFileSystem =
        (S3AFileSystem) s3DestPath.getFileSystem(getConfiguration());
    Path avroFilePath = new Path("s3a://sravani-data/summary/AvroData.avro");
    assertTrue("Avro file should be present in destination path",
        s3AFileSystem.exists(avroFilePath));
  }
}
