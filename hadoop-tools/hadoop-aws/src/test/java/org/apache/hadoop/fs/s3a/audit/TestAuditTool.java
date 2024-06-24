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

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.audit.S3LogParser.BUCKET_GROUP;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.REMOTEIP_GROUP;
import static org.apache.hadoop.fs.s3a.audit.TestS3AAuditLogMergerAndParser.SAMPLE_LOG_ENTRY;

/**
 * AuditTool tests.
 */
public class TestAuditTool extends HadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAuditTool.class);

  private final Configuration conf = new Configuration();

  /**
   * The audit tool.
   */
  private AuditTool auditTool;

  /**
   * Temporary directory to store the sample files; should be under target/
   * though IDEs may put it elsewhere.
   */
  private File sampleDir;

  /**
   * Sample directories and files to test.
   */
  private File sampleFile;

  private File sampleDestDir;

  @Before
  public void setup() throws Exception {
    auditTool = new AuditTool(conf);
  }

  /**
   * Testing run method in AuditTool class by passing source and destination
   * paths.
   */
  @Test
  public void testRun() throws Exception {
    sampleDir = Files.createTempDirectory("sampleDir").toFile();
    sampleFile = File.createTempFile("sampleFile", ".txt", sampleDir);
    try (FileWriter fw = new FileWriter(sampleFile)) {
      fw.write(SAMPLE_LOG_ENTRY);
      fw.flush();
    }
    sampleDestDir = Files.createTempDirectory("sampleDestDir").toFile();
    Path logsPath = new Path(sampleDir.toURI());
    Path destPath = new Path(sampleDestDir.toURI());
    String[] args = {logsPath.toString(), destPath.toString()};
    auditTool.run(args);
    FileSystem fileSystem = destPath.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> listOfDestFiles =
        fileSystem.listFiles(destPath, true);
    Path expectedPath = new Path(destPath, "AvroData.avro");
    fileSystem.open(expectedPath);

    File avroFile = new File(expectedPath.toUri());

    //DeSerializing the objects
    DatumReader<AvroS3LogEntryRecord> datumReader =
        new SpecificDatumReader<>(AvroS3LogEntryRecord.class);

    //Instantiating DataFileReader
    DataFileReader<AvroS3LogEntryRecord> dataFileReader =
        new DataFileReader<>(avroFile, datumReader);

    AvroS3LogEntryRecord record = new AvroS3LogEntryRecord();
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      Assertions.assertThat(record.get(BUCKET_GROUP))
          .describedAs(BUCKET_GROUP)
          .extracting(Object::toString)
          .isEqualTo("bucket-london");

      //verifying the remoteip from generated avro data
      Assertions.assertThat(record.get(REMOTEIP_GROUP))
          .describedAs(BUCKET_GROUP)
          .extracting(Object::toString)
          .isEqualTo("109.157.171.174");
    }
  }
}
