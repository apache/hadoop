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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestHarIndexParser extends TestCase {
  final static Log LOG = LogFactory.getLog(TestHarIndexParser.class);
  File indexFile = null;

  protected void setUp() throws FileNotFoundException, IOException {
    LOG.info("TestHarIndexParser.setUp()");
    indexFile = File.createTempFile("harindex", ".tmp");
    indexFile.deleteOnExit();
    OutputStreamWriter out = new OutputStreamWriter(
      new FileOutputStream(indexFile),
      Charset.forName("UTF-8"));
    out.write("%2F dir 1282018162460+0+493+hadoop+hadoop 0 0 f1 f2 f3 f4\n");
    out.write("%2Ff1 file part-0 0 1024 1282018141145+1282018140822+420+hadoop+hadoop\n");
    out.write("%2Ff3 file part-0 2048 1024 1282018148590+1282018148255+420+hadoop+hadoop\n");
    out.write("%2Ff2 file part-0 1024 1024 1282018144198+1282018143852+420+hadoop+hadoop\n");
    out.write("%2Ff4 file part-1 0 1024000 1282018162959+1282018162460+420+hadoop+hadoop\n");
    out.flush();
    out.close();
  }

  protected void tearDown() {
    LOG.info("TestHarIndexParser.tearDown()");
    if (indexFile != null)
      indexFile.delete();
  }

  public void testHarIndexParser()
    throws UnsupportedEncodingException, IOException {
    LOG.info("testHarIndexParser started.");
    InputStream in = new FileInputStream(indexFile);
    long size = indexFile.length();
    HarIndex parser = new HarIndex(in, size);

    HarIndex.IndexEntry entry = parser.findEntry("part-0", 2100);
    assertEquals("/f3", entry.fileName);

    LOG.info("testHarIndexParser finished.");
  }
}
