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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.sqoop.lib.RecordParser;
import org.apache.hadoop.sqoop.lib.SqoopRecord;

/**
 * Converts an input record from a string representation to a parsed Sqoop record
 * and emits that DBWritable to the DBOutputFormat for writeback to the database.
 */
public class TextExportMapper
    extends AutoProgressMapper<LongWritable, Text, SqoopRecord, NullWritable> {

  private SqoopRecord recordImpl;

  public TextExportMapper() {
  }

  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();

    // Instantiate a copy of the user's class to hold and parse the record.
    String recordClassName = conf.get(ExportJob.SQOOP_EXPORT_TABLE_CLASS_KEY);
    if (null == recordClassName) {
      throw new IOException("Export table class name ("
          + ExportJob.SQOOP_EXPORT_TABLE_CLASS_KEY
          + ") is not set!");
    }

    try {
      Class cls = Class.forName(recordClassName, true,
          Thread.currentThread().getContextClassLoader());
      recordImpl = (SqoopRecord) ReflectionUtils.newInstance(cls, conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    if (null == recordImpl) {
      throw new IOException("Could not instantiate object of type " + recordClassName);
    }
  }


  public void map(LongWritable key, Text val, Context context)
      throws IOException, InterruptedException {
    try {
      recordImpl.parse(val);
      context.write(recordImpl, NullWritable.get());
    } catch (RecordParser.ParseError pe) {
      throw new IOException("Could not parse record: " + val, pe);
    }
  }
}
