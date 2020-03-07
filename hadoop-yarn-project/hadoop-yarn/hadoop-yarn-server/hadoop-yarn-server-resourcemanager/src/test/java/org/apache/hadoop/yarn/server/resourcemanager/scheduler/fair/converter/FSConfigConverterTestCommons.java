/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile.AllocationFileWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

import static org.junit.Assert.assertTrue;

/**
 * Helper methods for FS->CS converter testing.
 *
 */
public class FSConfigConverterTestCommons {
  public final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();
  public final static String FS_ALLOC_FILE =
      new File(TEST_DIR, "test-fair-scheduler.xml").getAbsolutePath();
  public final static String YARN_SITE_XML =
      new File(TEST_DIR, "test-yarn-site.xml").getAbsolutePath();
  public final static String CONVERSION_RULES_FILE =
      new File(TEST_DIR, "test-conversion-rules.properties").getAbsolutePath();
  public final static String OUTPUT_DIR =
      new File(TEST_DIR, "conversion-output").getAbsolutePath();

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private PrintStream originalOutStream;
  private PrintStream originalErrStream;

  public FSConfigConverterTestCommons() {
    saveOriginalStreams();
    replaceStreamsWithByteArrays();
  }

  public void setUp() throws IOException {
    File d = new File(TEST_DIR, "conversion-output");
    if (d.exists()) {
      FileUtils.deleteDirectory(d);
    }
    boolean success = d.mkdirs();
    assertTrue("Can't create directory: " + d.getAbsolutePath(), success);
  }

  public void tearDown() {
    deleteTestFiles();
    restoreStreams();
  }

  private void saveOriginalStreams() {
    originalOutStream = System.out;
    originalErrStream = System.err;
  }

  private void replaceStreamsWithByteArrays() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  private void restoreStreams() {
    System.setOut(originalOutStream);
    System.setErr(originalErrStream);
  }

  ByteArrayOutputStream getErrContent() {
    return errContent;
  }

  ByteArrayOutputStream getStdOutContent() {
    return outContent;
  }

  public void deleteTestFiles() {
    //Files may not be created so we are not strict here!
    deleteFile(FS_ALLOC_FILE, false);
    deleteFile(YARN_SITE_XML, false);
    deleteFile(CONVERSION_RULES_FILE, false);
    deleteFile(OUTPUT_DIR, false);
  }

  private static void deleteFile(String f, boolean strict) {
    boolean delete = new File(f).delete();
    if (strict && !delete) {
      throw new RuntimeException("Can't delete test file: " + f);
    }
  }

  public static void setupFSConfigConversionFiles() throws IOException {
    configureFairSchedulerXml();
    configureYarnSiteXmlWithFsAllocFileDefined();
    configureDummyConversionRulesFile();
  }

  public static void configureFairSchedulerXml() {
    AllocationFileWriter.create()
        .disableQueueMaxAMShareDefault()
        .fairDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("root")
            .schedulingPolicy("fair")
            .weight(1.0f)
            .fairSharePreemptionTimeout(100)
            .minSharePreemptionTimeout(120)
            .fairSharePreemptionThreshold(0.5f)
            .build())
        .writeToFile(FS_ALLOC_FILE);
  }

  public static void configureEmptyFairSchedulerXml() {
    AllocationFileWriter.create()
        .writeToFile(FS_ALLOC_FILE);
  }

  public static void configureYarnSiteXmlWithFsAllocFileDefined()
      throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(YARN_SITE_XML));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<property>");
    out.println("<name>" + FairSchedulerConfiguration.ALLOCATION_FILE +
        "</name>");
    out.println("<value>" + FS_ALLOC_FILE + "</value>");
    out.println("</property>");
    out.close();
  }

  public static void configureEmptyYarnSiteXml() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(YARN_SITE_XML));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<property></property>");
    out.close();
  }

  public static void configureDummyConversionRulesFile() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(CONVERSION_RULES_FILE));
    out.println("dummy_key=dummy_value");
    out.close();
  }

  public static void configureInvalidConversionRulesFile() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(CONVERSION_RULES_FILE));
    out.println("bla");
    out.close();
  }

  public static void configureEmptyConversionRulesFile() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(CONVERSION_RULES_FILE));
    out.close();
  }
}
