/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.metrics.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.file.FileContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

/**
 * Add timestamp to {@link org.apache.hadoop.metrics.file.FileContext#emitRecord(String, String, OutputRecord)}.
 */
public class TimeStampingFileContext extends FileContext {
  // Copies bunch of FileContext here because writer and file are private in
  // superclass.
  private File file = null;
  private PrintWriter writer = null;
  private final SimpleDateFormat sdf;

  public TimeStampingFileContext() {
    super();
    this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
  }

  @Override
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
    String fileName = getAttribute(FILE_NAME_PROPERTY);
    if (fileName != null) {
      file = new File(fileName);
    }
  }

  @Override
  public void startMonitoring() throws IOException {
    if (file == null) {
      writer = new PrintWriter(new BufferedOutputStream(System.out));
    } else {
      writer = new PrintWriter(new FileWriter(file, true));
    }
    super.startMonitoring();
  }

  @Override
  public void stopMonitoring() {
    super.stopMonitoring();
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  private synchronized String iso8601() {
    return this.sdf.format(new Date());
  }

  @Override
  public void emitRecord(String contextName, String recordName,
      OutputRecord outRec) {
    writer.print(iso8601());
    writer.print(" ");
    writer.print(contextName);
    writer.print(".");
    writer.print(recordName);
    String separator = ": ";
    for (String tagName : outRec.getTagNames()) {
      writer.print(separator);
      separator = ", ";
      writer.print(tagName);
      writer.print("=");
      writer.print(outRec.getTag(tagName));
    }
    for (String metricName : outRec.getMetricNames()) {
      writer.print(separator);
      separator = ", ";
      writer.print(metricName);
      writer.print("=");
      writer.print(outRec.getMetric(metricName));
    }
    writer.println();
  }

  @Override
  public void flush() {
    writer.flush();
  }
}