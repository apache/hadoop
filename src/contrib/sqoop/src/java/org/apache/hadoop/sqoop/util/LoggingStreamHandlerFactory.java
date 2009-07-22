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

package org.apache.hadoop.sqoop.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A StreamHandlerFactory that takes the contents of a stream and writes
 * it to log4j.
 *
 */
public class LoggingStreamHandlerFactory implements StreamHandlerFactory {

  public static final Log LOG = LogFactory.getLog(LoggingStreamHandlerFactory.class.getName());

  private Log contextLog;

  public LoggingStreamHandlerFactory(final Log context) {
    if (null == context) {
      this.contextLog = LOG;
    } else {
      this.contextLog = context;
    }
  }

  private Thread child;

  public void processStream(InputStream is) {
    child = new LoggingThread(is);
    child.start();
  }

  public int join() throws InterruptedException {
    child.join();
    return 0; // always successful.
  }

  /**
   * Run a background thread that copies the contents of the stream
   * to the output context log.
   */
  private class LoggingThread extends Thread {

    private InputStream stream;

    LoggingThread(final InputStream is) {
      this.stream = is;
    }

    public void run() {
      InputStreamReader isr = new InputStreamReader(this.stream);
      BufferedReader r = new BufferedReader(isr);

      try {
        while (true) {
          String line = r.readLine();
          if (null == line) {
            break; // stream was closed by remote end.
          }

          LoggingStreamHandlerFactory.this.contextLog.info(line);
        }
      } catch (IOException ioe) {
        LOG.error("IOException reading from stream: " + ioe.toString());
      }

      try {
        r.close();
      } catch (IOException ioe) {
        LOG.warn("Error closing stream in LoggingStreamHandler: " + ioe.toString());
      }
    }
  }
}

