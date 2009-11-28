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

package org.apache.hadoop.sqoop.io;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.Formatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A BufferedWriter implementation that wraps around a SplittingOutputStream
 * and allows splitting of the underlying stream.
 * Splits occur at allowSplit() calls, or newLine() calls.
 */
public class SplittableBufferedWriter extends BufferedWriter {

  public static final Log LOG = LogFactory.getLog(
      SplittableBufferedWriter.class.getName());

  private SplittingOutputStream splitOutputStream;
  private boolean alwaysFlush;

  public SplittableBufferedWriter(
      final SplittingOutputStream splitOutputStream) {
    super(new OutputStreamWriter(splitOutputStream));

    this.splitOutputStream = splitOutputStream;
    this.alwaysFlush = false;
  }

  /** For testing */
  SplittableBufferedWriter(final SplittingOutputStream splitOutputStream,
      final boolean alwaysFlush) {
    super(new OutputStreamWriter(splitOutputStream));

    this.splitOutputStream = splitOutputStream;
    this.alwaysFlush = alwaysFlush;
  }

  public void newLine() throws IOException {
    super.newLine();
    this.allowSplit();
  }

  public void allowSplit() throws IOException {
    if (alwaysFlush) {
      this.flush();
    }
    if (this.splitOutputStream.wouldSplit()) {
      LOG.debug("Starting new split");
      this.flush();
      this.splitOutputStream.allowSplit();
    }
  }
}
