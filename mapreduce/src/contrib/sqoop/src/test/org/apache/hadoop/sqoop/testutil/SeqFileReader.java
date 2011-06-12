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

package org.apache.hadoop.sqoop.testutil;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Utility class to help with test cases. Just reads the first (k, v) pair
 * from a SequenceFile and returns the value part.
 * 
 *
 */
public final class SeqFileReader {

  public static final Log LOG = LogFactory.getLog(SeqFileReader.class.getName());

  public static Reader getSeqFileReader(String filename) throws IOException {
    // read from local filesystem
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Opening SequenceFile " + filename);
    return new SequenceFile.Reader(fs, new Path(filename), conf);
  }

  public static Object getFirstValue(String filename) throws IOException {
    Reader r = null;
    try {
      // read from local filesystem
      Configuration conf = new Configuration();
      conf.set("fs.default.name", "file:///");
      FileSystem fs = FileSystem.get(conf);
      r = new SequenceFile.Reader(fs, new Path(filename), conf);
      Object key = ReflectionUtils.newInstance(r.getKeyClass(), conf);
      Object val = ReflectionUtils.newInstance(r.getValueClass(), conf);
      LOG.info("Reading value of type " + r.getValueClassName()
          + " from SequenceFile " + filename);
      r.next(key);
      r.getCurrentValue(val);
      LOG.info("Value as string: " + val.toString());
      return val;
    } finally {
      if (null != r) {
        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("IOException during close: " + ioe.toString());
        }
      }
    }
  }
}

