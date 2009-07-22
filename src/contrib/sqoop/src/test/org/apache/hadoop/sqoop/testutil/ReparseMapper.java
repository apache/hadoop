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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.sqoop.lib.RecordParser;
import org.apache.hadoop.sqoop.lib.SqoopRecord;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * Test harness mapper. Instantiate the user's specific type, parse() the input 
 * line of text, and throw an IOException if the output toString() line of text
 * differs.
 */
public class ReparseMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, NullWritable> {

  public static final Log LOG = LogFactory.getLog(ReparseMapper.class.getName());

  public static final String USER_TYPE_NAME_KEY = "sqoop.user.class";

  private SqoopRecord userRecord;

  public void configure(JobConf job) {
    String userTypeName = job.get(USER_TYPE_NAME_KEY);
    if (null == userTypeName) {
      throw new RuntimeException("Unconfigured parameter: " + USER_TYPE_NAME_KEY);
    }

    LOG.info("User type name set to " + userTypeName);

    this.userRecord = null;

    try {
      Configuration conf = new Configuration();
      Class userClass = Class.forName(userTypeName, true,
          Thread.currentThread().getContextClassLoader());
      this.userRecord =
          (SqoopRecord) ReflectionUtils.newInstance(userClass, conf);
    } catch (ClassNotFoundException cnfe) {
      // handled by the next block.
      LOG.error("ClassNotFound exception: " + cnfe.toString());
    } catch (Exception e) {
      LOG.error("Got an exception reflecting user class: " + e.toString());
    }

    if (null == this.userRecord) {
      LOG.error("Could not instantiate user record of type " + userTypeName);
      throw new RuntimeException("Could not instantiate user record of type " + userTypeName);
    }
  }

  public void map(LongWritable key, Text val, OutputCollector<Text, NullWritable> out, Reporter r)
      throws IOException {

    LOG.info("Mapper input line: " + val.toString());

    try {
      // Use the user's record class to parse the line back in.
      userRecord.parse(val);
    } catch (RecordParser.ParseError pe) {
      LOG.error("Got parse error: " + pe.toString());
      throw new IOException(pe);
    }

    LOG.info("Mapper output line: " + userRecord.toString());

    out.collect(new Text(userRecord.toString()), NullWritable.get());

    if (!userRecord.toString().equals(val.toString() + "\n")) {
      // misparsed.
      throw new IOException("Returned string has value [" + userRecord.toString() + "] when ["
          + val.toString() + "\n] was expected.");
    }
  }
}

