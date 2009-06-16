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

<h2>Table of Contents</h2>
<ul>
<li><a href="#classpath">HBase, MapReduce and the CLASSPATH</a></li>
<li><a href="#sink">HBase as MapReduce job data source and sink</a></li>
<li><a href="#examples">Example Code</a></li>
</ul>

<h2><a name="classpath">HBase, MapReduce and the CLASSPATH</a></h2>

<p>MapReduce jobs deployed to a MapReduce cluster do not by default have access
to the HBase configuration under <code>$HBASE_CONF_DIR</code> nor to HBase classes.
You could add <code>hbase-site.xml</code> to $HADOOP_HOME/conf and add
<code>hbase-X.X.X.jar</code> to the <code>$HADOOP_HOME/lib</code> and copy these
changes across your cluster but the cleanest means of adding hbase configuration
and classes to the cluster <code>CLASSPATH</code> is by uncommenting
<code>HADOOP_CLASSPATH</code> in <code>$HADOOP_HOME/conf/hadoop-env.sh</code>
and adding the path to the hbase jar and <code>$HBASE_CONF_DIR</code> directory.
Then copy the amended configuration around the cluster.
You'll probably need to restart the MapReduce cluster if you want it to notice
the new configuration.
</p>

<p>For example, here is how you would amend <code>hadoop-env.sh</code> adding the
built hbase jar, hbase conf, and the <code>PerformanceEvaluation</code> class from
the built hbase test jar to the hadoop <code>CLASSPATH<code>:

<blockquote><pre># Extra Java CLASSPATH elements. Optional.
# export HADOOP_CLASSPATH=
export HADOOP_CLASSPATH=$HBASE_HOME/build/test:$HBASE_HOME/build/hbase-X.X.X.jar:$HBASE_HOME/build/hbase-X.X.X-test.jar:$HBASE_HOME/conf</pre></blockquote>

<p>Expand <code>$HBASE_HOME</code> in the above appropriately to suit your
local environment.</p>

<p>After copying the above change around your cluster, this is how you would run
the PerformanceEvaluation MR job to put up 4 clients (Presumes a ready mapreduce
cluster):

<blockquote><pre>$HADOOP_HOME/bin/hadoop org.apache.hadoop.hbase.PerformanceEvaluation sequentialWrite 4</pre></blockquote>

The PerformanceEvaluation class wil be found on the CLASSPATH because you
added <code>$HBASE_HOME/build/test</code> to HADOOP_CLASSPATH
</p>

<p>Another possibility, if for example you do not have access to hadoop-env.sh or
are unable to restart the hadoop cluster, is bundling the hbase jar into a mapreduce
job jar adding it and its dependencies under the job jar <code>lib/</code>
directory and the hbase conf into a job jar <code>conf/</code> directory.
</a>

<h2><a name="sink">HBase as MapReduce job data source and sink</a></h2>

<p>HBase can be used as a data source, {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat TableInputFormat},
and data sink, {@link org.apache.hadoop.hbase.mapreduce.TableOutputFormat TableOutputFormat}, for MapReduce jobs.
Writing MapReduce jobs that read or write HBase, you'll probably want to subclass
{@link org.apache.hadoop.hbase.mapreduce.TableMap TableMap} and/or
{@link org.apache.hadoop.hbase.mapreduce.TableReduce TableReduce}.  See the do-nothing
pass-through classes {@link org.apache.hadoop.hbase.mapreduce.IdentityTableMap IdentityTableMap} and
{@link org.apache.hadoop.hbase.mapreduce.IdentityTableReduce IdentityTableReduce} for basic usage.  For a more
involved example, see {@link org.apache.hadoop.hbase.mapreduce.BuildTableIndex BuildTableIndex}
or review the <code>org.apache.hadoop.hbase.mapreduce.TestTableMapReduce</code> unit test.
</p>

<p>Running mapreduce jobs that have hbase as source or sink, you'll need to
specify source/sink table and column names in your configuration.</p>

<p>Reading from hbase, the TableInputFormat asks hbase for the list of
regions and makes a map-per-region or <code>mapred.map.tasks maps</code>,
whichever is smaller (If your job only has two maps, up mapred.map.tasks
to a number > number of regions). Maps will run on the adjacent TaskTracker
if you are running a TaskTracer and RegionServer per node.
Writing, it may make sense to avoid the reduce step and write yourself back into
hbase from inside your map. You'd do this when your job does not need the sort
and collation that mapreduce does on the map emitted data; on insert,
hbase 'sorts' so there is no point double-sorting (and shuffling data around
your mapreduce cluster) unless you need to. If you do not need the reduce,
you might just have your map emit counts of records processed just so the
framework's report at the end of your job has meaning or set the number of
reduces to zero and use TableOutputFormat. See example code
below. If running the reduce step makes sense in your case, its usually better
to have lots of reducers so load is spread across the hbase cluster.</p>

<p>There is also a new hbase partitioner that will run as many reducers as
currently existing regions.  The 
{@link org.apache.hadoop.hbase.mapreduce.HRegionPartitioner} is suitable
when your table is large and your upload is not such that it will greatly
alter the number of existing regions when done; other use the default
partitioner.
</p>

<h2><a name="examples">Example Code</a></h2>
<h3>Sample Row Counter</h3>
<p>See {@link org.apache.hadoop.hbase.mapreduce.RowCounter}.  You should be able to run
it by doing: <code>% ./bin/hadoop jar hbase-X.X.X.jar</code>.  This will invoke
the hbase MapReduce Driver class.  Select 'rowcounter' from the choice of jobs
offered. You may need to add the hbase conf directory to <code>$HADOOP_HOME/conf/hadoop-env.sh#HADOOP_CLASSPATH</code>
so the rowcounter gets pointed at the right hbase cluster (or, build a new jar
with an appropriate hbase-site.xml built into your job jar).
</p>
<h3>PerformanceEvaluation</h3>
<p>See org.apache.hadoop.hbase.PerformanceEvaluation from hbase src/test.  It runs
a mapreduce job to run concurrent clients reading and writing hbase.
</p>

<h3>Sample MR Bulk Uploader</h3>
<p>A students/classes example based on a contribution by Naama Kraus with logs of
documentation can be found over in src/examples/mapred.
Its the <code>org.apache.hadoop.hbase.mapreduce.SampleUploader</code> class.
Just copy it under src/java/org/apache/hadoop/hbase/mapred to compile and try it
(until we start generating an hbase examples jar).  The class reads a data file
from HDFS and per line, does an upload to HBase using TableReduce.
Read the class comment for specification of inputs, prerequisites, etc.
</p>

<h3>Example to bulk import/load a text file into an HTable
</h3>

<p>Here's a sample program from 
<a href="http://www.spicylogic.com/allenday/blog/category/computing/distributed-systems/hadoop/hbase/">Allen Day</a>
that takes an HDFS text file path and an HBase table name as inputs, and loads the contents of the text file to the table
all up in the map phase.
</p>

<blockquote><pre>
package com.spicylogic.hbase;
package org.apache.hadoop.hbase.mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class that adds the parsed line from the input to hbase
 * in the map function.  Map has no emissions and job
 * has no reduce.
 *&#x2f;
public class BulkImport implements Tool {
  private static final String NAME = "BulkImport";
  private Configuration conf;

  public static class InnerMap extends MapReduceBase implements Mapper&lt;LongWritable, Text, Text, Text> {
    private HTable table;
    private HBaseConfiguration HBconf;

    public void map(LongWritable key, Text value,
        OutputCollector&lt;Text, Text> output, Reporter reporter)
    throws IOException {
      if ( table == null )
        throw new IOException("table is null");
      
      // Split input line on tab character
      String [] splits = value.toString().split("\t");
      if ( splits.length != 4 )
        return;
      
      String rowID = splits[0];
      int timestamp  = Integer.parseInt( splits[1] );
      String colID = splits[2];
      String cellValue = splits[3];

      reporter.setStatus("Map emitting cell for row='" + rowID +
          "', column='" + colID + "', time='" + timestamp + "'");

      BatchUpdate bu = new BatchUpdate( rowID );
      if ( timestamp > 0 )
        bu.setTimestamp( timestamp );

      bu.put(colID, cellValue.getBytes());      
      table.commit( bu );      
    }

    public void configure(JobConf job) {
      HBconf = new HBaseConfiguration(job);
      try {
        table = new HTable( HBconf, job.get("input.table") );
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  public JobConf createSubmittableJob(String[] args) {
    JobConf c = new JobConf(getConf(), BulkImport.class);
    c.setJobName(NAME);
    FileInputFormat.setInputPaths(c, new Path(args[0]));

    c.set("input.table", args[1]);
    c.setMapperClass(InnerMap.class);
    c.setNumReduceTasks(0);
    c.setOutputFormat(NullOutputFormat.class);
    return c;
  }
  
  static int printUsage() {
    System.err.println("Usage: " + NAME + " &lt;input> &lt;table_name>");
    System.err.println("\twhere &lt;input> is a tab-delimited text file with 4 columns.");
    System.err.println("\t\tcolumn 1 = row ID");
    System.err.println("\t\tcolumn 2 = timestamp (use a negative value for current time)");
    System.err.println("\t\tcolumn 3 = column ID");
    System.err.println("\t\tcolumn 4 = cell value");
    return -1;
  } 

  public int run(@SuppressWarnings("unused") String[] args) throws Exception {
    // Make sure there are exactly 3 parameters left.
    if (args.length != 2) {
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
    int errCode = ToolRunner.run(new Configuration(), new BulkImport(), args);
    System.exit(errCode);
  }
}
</pre></blockquote>

*/
package org.apache.hadoop.hbase.mapreduce;
