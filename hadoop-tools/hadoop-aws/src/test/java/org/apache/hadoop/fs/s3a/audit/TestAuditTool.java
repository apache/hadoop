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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;

import static org.apache.hadoop.fs.s3a.audit.TestS3AAuditLogMergerAndParser.SAMPLE_LOG_ENTRY;

/**
 * This will implement tests on AuditTool class.
 */
public class TestAuditTool extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAuditTool.class);

  private final AuditTool auditTool = new AuditTool();

  /**
   * Sample directories and files to test.
   */
  private File sampleFile;
  private File sampleDir;
  private File sampleDestDir;

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
    String[] args = {destPath.toString(), logsPath.toString()};
    auditTool.run(args);
    FileSystem fileSystem = destPath.getFileSystem(getConfiguration());
    RemoteIterator<LocatedFileStatus> listOfDestFiles =
        fileSystem.listFiles(destPath, true);
    Path expectedPath = new Path(destPath, "AvroData.avro");
    fileSystem.open(expectedPath);

    File avroFile = new File(expectedPath.toUri());

    //DeSerializing the objects
    DatumReader<AvroS3LogEntryRecord> datumReader =
            new SpecificDatumReader<AvroS3LogEntryRecord>(AvroS3LogEntryRecord.class);

    //Instantiating DataFileReader
    DataFileReader<AvroS3LogEntryRecord> dataFileReader =
            new DataFileReader<AvroS3LogEntryRecord>(avroFile, datumReader);
    AvroS3LogEntryRecord avroS3LogEntryRecord = null;

    while (dataFileReader.hasNext()) {
      avroS3LogEntryRecord = dataFileReader.next(avroS3LogEntryRecord);
      //verifying the bucket from generated avro data
      assertEquals("the expected and actual results should be same",
              "bucket-london", avroS3LogEntryRecord.get("bucket").toString());
      //verifying the remoteip from generated avro data
      assertEquals("the expected and actual results should be same",
              "109.157.171.174", avroS3LogEntryRecord.get("remoteip").toString());
    }
  }
}
