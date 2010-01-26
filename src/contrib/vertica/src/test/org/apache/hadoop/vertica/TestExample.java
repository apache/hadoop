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

package org.apache.hadoop.vertica;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.vertica.VerticaConfiguration;
import org.apache.hadoop.vertica.VerticaInputFormat;
import org.apache.hadoop.vertica.VerticaOutputFormat;
import org.apache.hadoop.vertica.VerticaRecord;

public class TestExample extends VerticaTestCase implements Tool {

  public TestExample(String name) {
    super(name);
  }

  public static class Map extends
      Mapper<LongWritable, VerticaRecord, Text, DoubleWritable> {

    public void map(LongWritable key, VerticaRecord value, Context context)
        throws IOException, InterruptedException {
      List<Object> record = value.getValues();
      context.write(new Text((String) record.get(1)), new DoubleWritable(
          (Long) record.get(0)));
    }
  }

  public static class Reduce extends
      Reducer<Text, DoubleWritable, Text, VerticaRecord> {
    VerticaRecord record = null;

    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      try {
        record = VerticaOutputFormat.getValue(context.getConfiguration());
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    protected void reduce(Text key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      if (record == null) {
        throw new IOException("No output record found");
      }
      
      record.set(0, 125, true);
      record.set(1, true, true);
      record.set(2, 'c', true);
      record.set(3, Calendar.getInstance().getTime(), true);
      record.set(4, 234.526, true);
      record.set(5, Calendar.getInstance().getTime(), true);
      record.set(6, "foobar string", true);
      record.set(7, new byte[10], true);
      context.write(new Text("mrtarget"), record);
    }
  }

  public Job getJob() throws IOException {
    Configuration conf = new Configuration(true);
    Cluster cluster = new Cluster(conf);
    Job job = Job.getInstance(cluster);
    
    conf = job.getConfiguration();
    conf.set("mapreduce.job.tracker", "local");

    job.setJarByClass(TestExample.class);
    job.setJobName("vertica test");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VerticaRecord.class);
    job.setInputFormatClass(VerticaInputFormat.class);
    job.setOutputFormatClass(VerticaOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    VerticaOutputFormat.setOutput(job, "mrtarget", true, "a int", "b boolean",
        "c char(1)", "d date", "f float", "t timestamp", "v varchar",
        "z varbinary");
    VerticaConfiguration.configureVertica(conf,
        new String[] { AllTests.getHostname() }, AllTests.getDatabase(),
        AllTests.getUsername(), AllTests.getPassword());
    return job;
  }

  @SuppressWarnings("serial")
  public void testExample() throws Exception {
    if(!AllTests.isSetup()) {
      return;
    }

    Job job = getJob();
    VerticaInputFormat.setInput(job, "select * from mrsource");
    job.waitForCompletion(true);

    job = getJob();
    VerticaInputFormat.setInput(job, "select * from mrsource where key = ?",
        "select distinct key from mrsource");
    job.waitForCompletion(true);

    job = getJob();
    Collection<List<Object>> params = new HashSet<List<Object>>() {
    };
    List<Object> param = new ArrayList<Object>();
    param.add(new Integer(0));
    params.add(param);
    VerticaInputFormat.setInput(job, "select * from mrsource where key = ?",
        params);
    job.waitForCompletion(true);

    job = getJob();
    VerticaInputFormat.setInput(job, "select * from mrsource where key = ?",
        "0", "1", "2");
    job.waitForCompletion(true);
   
    VerticaOutputFormat.optimize(job.getConfiguration());
  }

  @Override
  public int run(String[] arg0) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
    // TODO Auto-generated method stub

  }
}
