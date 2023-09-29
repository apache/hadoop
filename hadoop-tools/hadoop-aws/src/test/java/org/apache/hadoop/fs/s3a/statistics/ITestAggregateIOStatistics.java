/*
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

package org.apache.hadoop.fs.s3a.statistics;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.util.JsonSerialization;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

/**
 * Saves, loads and logs the aggregate IOStatistics as collected in this
 * process.
 */
public class ITestAggregateIOStatistics extends AbstractS3ATestBase {

  @Test
  public void testSaveStatisticsLocal() throws Throwable {
    IOStatisticsSnapshot iostats = FILESYSTEM_IOSTATS;
    iostats.aggregate(getFileSystem().getIOStatistics());
    JsonSerialization<IOStatisticsSnapshot> serializer
        = IOStatisticsSnapshot.serializer();
    File outputDir = createOutputDir();
    File file = new File(outputDir, outputFilename());
    serializer.save(file, iostats);
    IOStatisticsSnapshot loaded = serializer.load(file);
    String s = serializer.toString(loaded);
    LOG.info("Deserialized statistics in {}\n{}",
        file, s);
  }

  @Test
  public void testSaveStatisticsS3() throws Throwable {
    IOStatisticsSnapshot iostats = FILESYSTEM_IOSTATS;
    JsonSerialization<IOStatisticsSnapshot> serializer
        = IOStatisticsSnapshot.serializer();
    Path path = methodPath();
    serializer.save(getFileSystem(), path, iostats, true);
    IOStatisticsSnapshot loaded = serializer.load(getFileSystem(), path);
    String s = serializer.toString(loaded);
    LOG.info("Deserialized statistics in {}\n{}",
        path, s);
  }

  protected File createOutputDir() {
    String target = System.getProperty("test.build.dir", "target");
    File buildDir = new File(target,
        this.getClass().getSimpleName()).getAbsoluteFile();
    buildDir.mkdirs();
    return buildDir;
  }

  protected String outputFilename() {
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral('-')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral('.')
        .appendValue(MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral('.')
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .toFormatter();
    return String.format("iostats-%s.json",
        now.format(formatter));

  }
}
