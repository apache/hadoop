/*
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
/**
Provides HBase <a href="http://wiki.apache.org/hadoop/HadoopMapReduce">MapReduce</a>
Input/OutputFormats, a table indexing MapReduce job, and utility

<h2> HBase, MapReduce and the CLASSPATH </h2>

<p>MapReduce jobs deployed to a MapReduce cluster do not by default have access
to the HBase configuration under <code>$HBASE_CONF_DIR</code> nor to HBase classes.
You could add <code>hbase-site.xml</code> to $HADOOP_HOME/conf and add
<code>hbase-X.X.X.jar</code>
to the <code>$HADOOP_HOME/lib</code> and copy these changes across your cluster but the
cleanest means of adding hbase configuration and classes to the cluster
<code>CLASSPATH</code>
is by uncommenting <code>HADOOP_CLASSPATH</code> in <code>$HADOOP_HOME/conf/hadoop-env.sh</code>
and adding the path to the hbase jar and <code>$HBASE_CONF_DIR</code> directory.
Then copy the amended configuration around the cluster.
You'll probably need to restart the MapReduce cluster if you want it to notice
the new configuration (You may not have to).
</p>

<p>For example, here is how you would amend <code>hadoop-env.sh</code> adding
the hbase jar, conf, and the <code>PerformanceEvaluation</code> class from hbase test
classes to the hadoop <code>CLASSPATH<code>

<blockquote><pre># Extra Java CLASSPATH elements.  Optional.
# export HADOOP_CLASSPATH=
export HADOOP_CLASSPATH=$HBASE_HOME/build/test:$HBASE_HOME/build/hbase-X.X.X.jar:$HBASE_HOME/build/hbase-X.X.X-test.jar:$HBASE_HOME/conf</pre></blockquote>

<p>Expand $HBASE_HOME appropriately in the in accordance with your local environment.</p>

<p>This is how you would run the PerformanceEvaluation MR job to put up 4 clients:

<blockquote><pre>$HADOOP_HOME/bin/hadoop org.apache.hadoop.hbase.PerformanceEvaluation sequentialWrite 4</pre></blockquote>

The PerformanceEvaluation class wil be found on the CLASSPATH because you added $HBASE_HOME/build/test to HADOOP_CLASSPATH
</p>

<h2>HBase as MapReduce job data source and sink</h2>

<p>HBase can be used as a data source, {@link org.apache.hadoop.hbase.mapred.TableInputFormat TableInputFormat},
and data sink, {@link org.apache.hadoop.hbase.mapred.TableOutputFormat TableOutputFormat}, for MapReduce jobs.
Writing MapReduce jobs that read or write HBase, you'll probably want to subclass
{@link org.apache.hadoop.hbase.mapred.TableMap TableMap} and/or
{@link org.apache.hadoop.hbase.mapred.TableReduce TableReduce}.  See the do-nothing
pass-through classes {@link org.apache.hadoop.hbase.mapred.IdentityTableMap IdentityTableMap} and
{@link org.apache.hadoop.hbase.mapred.IdentityTableReduce IdentityTableReduce} for basic usage.  For a more
involved example, see {@link org.apache.hadoop.hbase.mapred.BuildTableIndex BuildTableIndex}
or review the <code>org.apache.hadoop.hbase.mapred.TestTableMapReduce</code> unit test.
</p>

<p>Running mapreduce jobs that have hbase as source or sink, you'll need to
specify source/sink table and column names in your configuration.</p>

<p>Reading from hbase, the !TableInputFormat asks hbase for the list of
regions and makes a map-per-region.  Writing, its better to have lots of
reducers so load is spread across the hbase cluster.
</p>

<h2> Sample MR Bulk Uploader </h2>
<p>Read the class comment below for specification of inputs, prerequisites, etc.
</p>
<blockquote><pre>package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Sample uploader.
 * 
 * This is EXAMPLE code.  You will need to change it to work for your context.
 * 
 * Uses TableReduce to put the data into hbase. Change the InputFormat to suit
 * your data. Use the map to massage the input so it fits hbase.  Currently its
 * just a pass-through map.  In the reduce, you need to output a row and a
 * map of columns to cells.  Change map and reduce to suit your input.
 * 
 * &lt;p>The below is wired up to handle an input whose format is a text file
 * which has a line format as follow:
 * &lt;pre>
 * row columnname columndata
 * &lt;/pre>
 * 
 * &lt;p>The table and columnfamily we're to insert into must preexist.
 * 
 * &lt;p> To run, edit your hadoop-env.sh and add hbase classes and conf to your
 * HADOOP_CLASSPATH.  For example:
 * &lt;pre>
 * export HADOOP_CLASSPATH=/Users/stack/Documents/checkouts/hbase/branches/0.1/build/classes:/Users/stack/Documents/checkouts/hbase/branches/0.1/conf
 * &lt;/pre>
 * &lt;p>Restart your MR cluster after making the following change (You need to 
 * be running in pseudo-distributed mode at a minimum for the hadoop to see
 * the above additions to your CLASSPATH).
 * 
 * &lt;p>Start up your hbase cluster.
 * 
 * &lt;p>Next do the following to start the MR job:
 * &lt;pre>
 * ./bin/hadoop org.apache.hadoop.hbase.mapred.SampleUploader /tmp/input.txt TABLE_NAME
 * &lt;/pre>
 * 
 * &lt;p>This code was written against hbase 0.1 branch.
 *&#x2f;
public class SampleUploader extends MapReduceBase
implements Mapper<LongWritable, Text, Text, MapWritable>, Tool {
  private static final String NAME = "SampleUploader";
  private Configuration conf;

  public JobConf createSubmittableJob(String[] args) {
    JobConf c = new JobConf(getConf(), SampleUploader.class);
    c.setJobName(NAME);
    c.setInputPath(new Path(args[0]));
    c.setMapperClass(this.getClass());
    c.setMapOutputKeyClass(Text.class);
    c.setMapOutputValueClass(MapWritable.class);
    c.setReducerClass(TableUploader.class);
    TableReduce.initJob(args[1], TableUploader.class, c);
    return c;
  } 

  public void map(LongWritable k, Text v,
    OutputCollector<Text, MapWritable> output, Reporter r)
  throws IOException {
    // Lines are space-delimited; first item is row, next the columnname and
    // then the third the cell value.
    String tmp = v.toString();
    if (tmp.length() == 0) {
      return;
    }
    String [] splits = v.toString().split(" ");
    MapWritable mw = new MapWritable();
    mw.put(new Text(splits[1]),
      new ImmutableBytesWritable(splits[2].getBytes()));
    String row = splits[0];
    r.setStatus("Map emitting " + row + " for record " + k.toString());
    output.collect(new Text(row), mw);
  }
  
  public static class TableUploader
  extends TableReduce&lt;Text, MapWritable> {
    &#x40;Override
    public void reduce(Text k, Iterator<MapWritable> v,
      OutputCollector<Text, MapWritable> output, Reporter r)
    throws IOException {
      while (v.hasNext()) {
        r.setStatus("Reducer committing " + k);
        output.collect(k, v.next());
      }
    }
  }
  
  static int printUsage() {
    System.out.println(NAME + "&lt;input> &lt;table_name>");
    return -1;
  } 
    
  public int run(@SuppressWarnings("unused") String[] args) throws Exception {
    // Make sure there are exactly 2 parameters left.
    if (args.length != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
        args.length + " instead of 2.");
      return printUsage();
    }
    JobClient.runJob(createSubmittableJob(args));
    return 0;
  }
    
  public Configuration getConf() {
    return this.conf;
  } 
  
  public void setConf(final Configuration c) {
    this.conf = c;
  }

  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(new Configuration(), new SampleUploader(),
      args);
    System.exit(errCode);
  }
}
</pre></blockquote>
*/
package org.apache.hadoop.hbase.mapred;
