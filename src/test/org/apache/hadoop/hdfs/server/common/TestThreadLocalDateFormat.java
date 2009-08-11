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
package org.apache.hadoop.hdfs.server.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import junit.framework.TestCase;

/**
 * Test for {@link ThreadLocalDateFormat}
 */
public class TestThreadLocalDateFormat extends TestCase {
  private static final int TOTAL_THREADS = 3;
  private static final Log LOG = LogFactory.getLog(TestThreadLocalDateFormat.class);
  private static final ThreadLocalDateFormat TDF = new ThreadLocalDateFormat(
      "dd-MM-yyyy HH:mm:ss:S Z");
  private static volatile boolean failed = false;
  private final static Random rand = new Random();

  private static synchronized void setFailed() {
    failed = true;
  }

  /**
   * Run formatting and parsing test and look for multi threaded access related
   * failures
   */
  private void runTest(final SimpleDateFormat df) {
    while (!failed) {
      try {
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date(rand.nextInt(Integer.MAX_VALUE));
        String s1 = df.format(date);
        Date parsedDate = df.parse(s1);
        String s2 = df.format(parsedDate);
        if (!s1.equals(s2)) {
          LOG.warn("Parse failed, actual /" + s2 + "/ expected /" + s1 + "/");
          setFailed();
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        setFailed();
        LOG.warn("exception ", e);
      } catch (ParseException e) {
        LOG.warn("Parsing failed ", e);
        setFailed();
      } catch (Exception e) {
        LOG.warn("Unknown exception", e);
        setFailed();
      }
    }
  }

  /**
   * {@link SimpleDateFormat} when using with multiple threads has following
   * issues:
   * <ul>
   * <li>format method throws {@link ArrayIndexOutOfBoundsException}
   * <li>parse method throws {@link ParseException} or returns invalid parse
   * </ul>
   * This test shows ThreadLocal based implementation of
   * {@link SimpleDateFormat} does not have these issues.
   * 
   * @throws InterruptedException
   */
  public void testDateFormat() throws InterruptedException {
    for (int i = 0; i < TOTAL_THREADS; i++) {
      Thread thread = new Thread() {
        public void run() {
          runTest(TDF.get());
        }
      };
      thread.start();
    }

    // Wait up to 30 seconds for failure to occur
    long endTime = System.currentTimeMillis() + 30 * 1000;
    while (!failed && endTime > System.currentTimeMillis()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.debug("Exception", ie);
      }
    }
    assertFalse(failed);
  }
}
