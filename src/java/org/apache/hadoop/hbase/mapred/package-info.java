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
<code>hbase-X.X.X.jar</code> to the <code>$HADOOP_HOME/lib</code> and copy these
changes across your cluster but the cleanest means of adding hbase configuration
and classes to the cluster <code>CLASSPATH</code> is by uncommenting
<code>HADOOP_CLASSPATH</code> in <code>$HADOOP_HOME/conf/hadoop-env.sh</code>
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

<p>Expand $HBASE_HOME in the above appropriately to suit your local environment.</p>

<p>This is how you would run the PerformanceEvaluation MR job to put up 4 clients:

<blockquote><pre>$HADOOP_HOME/bin/hadoop org.apache.hadoop.hbase.PerformanceEvaluation sequentialWrite 4</pre></blockquote>

The PerformanceEvaluation class wil be found on the CLASSPATH because you added $HBASE_HOME/build/test to HADOOP_CLASSPATH
</p>
<p>NOTE: While previous it used to be possible to bundle the hbase.jar up inside the job jar you submit to hadoop, as of
0.2.0RC2, this is no longer so. See <a href="https://issues.apache.org/jira/browse/HBASE-797">HBASE-797</a>. 

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

<p>Reading from hbase, the TableInputFormat asks hbase for the list of
regions and makes a map-per-region.  
Writing, it may make sense to avoid the reduce step and write back into hbase from inside your map. You'd do this when your job does not need the sort and collation that MR does inside in its reduce; on insert,
hbase sorts so no point double-sorting (and shuffling data around your MR cluster) unless you need to. If you do not need the reduce, you might just have your map emit counts of records processed just so the
framework can emit that nice report of records processed when the job is done. See example code below. If running the reduce step makes sense in your case, its better to have lots of reducers so load is spread
across the hbase cluster.</p>

<p>There is also a partitioner
that will run as many reducers as currently existing regions.  The
partitioner HRegionPartitioner is suitable when your table is large
and your upload is not such that it will greatly alter the number of existing
regions after its done.
</p>

<h2>Example Code</h2>
<h3>Sample Row Counter</h3>
<p>See {@link org.apache.hadoop.hbase.mapred.RowCounter}.  You should be able to run
it by doing: <code>% ./bin/hadoop jar hbase-X.X.X.jar</code>.  This will invoke
the hbase MapReduce Driver class.  Select 'rowcounter' from the choice of jobs
offered.
</p>

<h3>Example to bulk import/load a text file into an HTable
</h3>

<p>Here's a sample program from [WWW] Allen Day that takes an HDFS text file path and an HBase table name as inputs, and loads the contents of the text file to the table.
</p>

<blockquote><pre>package com.spicylogic.hbase;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkImport implements Tool {
  private static final String NAME = "BulkImport";
  private Configuration conf;

  public static class InnerMap extends MapReduceBase implements Mapper&lt;LongWritable, Text, Text, Text> {
    private HTable table;
    private HBaseConfiguration HBconf;

    public void map(LongWritable key, Text value, OutputCollector&lt;&lt;&lt;&lt;&lt;&lt;&lt;&lt;Text, Text> output, Reporter reporter) throws IOException {
      if ( table == null )
        throw new IOException("table is null");
      
      String [] splits = value.toString().split("\t");
      if ( splits.length != 4 )
        return;

      String rowID     = splits[0];
      int timestamp    = Integer.parseInt( splits[1] );
      String colID     = splits[2];
      String cellValue = splits[3];

      reporter.setStatus("Map emitting cell for row='" + rowID + "', column='" + colID + "', time='" + timestamp + "'");

      BatchUpdate bu = new BatchUpdate( rowID );
      if ( timestamp > 0 )
        bu.setTimestamp( timestamp );

      bu.put(colID, cellValue.getBytes());      
      table.commit( bu );      
    }
    public void configure(JobConf job) {
      HBconf = new HBaseConfiguration();
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
    c.setInputPath(new Path(args[0]));

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

<h3>Example to map rows/column families between two HTables
</h3>

<p>Here another sample program from [WWW] Allen Day that will iterate over all rows in one table for specified column families and insert those rows/columns to a second table.
</p>

<blockquote><pre>package com.spicylogic.hbase;
import java.io.IOException;

public class BulkCopy extends TableMap&lt;Text, Text> implements Tool {
  static final String NAME = "bulkcopy";  
  private Configuration conf;
  
  public void map(ImmutableBytesWritable row, RowResult value, OutputCollector&lt;Text, Text> output, Reporter reporter) throws IOException {
    HTable table = new HTable(new HBaseConfiguration(), conf.get("output.table"));
    if ( table == null ) {
      throw new IOException("output table is null");
    }

    BatchUpdate bu = new BatchUpdate( row.get() );

    boolean content = false;
    for (Map.Entry&lt;byte [], Cell> e: value.entrySet()) {
      Cell cell = e.getValue();
      if (cell != null && cell.getValue().length > 0) {
        bu.put(e.getKey(), cell.getValue());
      }
    }
    table.commit( bu );
  }

  public JobConf createSubmittableJob(String[] args) throws IOException {
    JobConf c = new JobConf(getConf(), BulkExport.class);
    //table = new HTable(new HBaseConfiguration(), args[2]);
    c.set("output.table", args[2]);
    c.setJobName(NAME);
    // Columns are space delimited
    StringBuilder sb = new StringBuilder();
    final int columnoffset = 3;
    for (int i = columnoffset; i &lt; args.length; i++) {
      if (i > columnoffset) {
        sb.append(" ");
      }
      sb.append(args[i]);
    }
    // Second argument is the table name.
    TableMap.initJob(args[1], sb.toString(), this.getClass(),
    Text.class, Text.class, c);
    c.setReducerClass(IdentityReducer.class);
    // First arg is the output directory.
    c.setOutputPath(new Path(args[0]));
    return c;
  }
  
  static int printUsage() {
    System.out.println(NAME +" &lt;outputdir> &lt;input tablename> &lt;output tablename> &lt;column1> [&lt;column2>...]");
    return -1;
  }
  
  public int run(final String[] args) throws Exception {
    // Make sure there are at least 3 parameters
    if (args.length &lt; 3) {
      System.err.println("ERROR: Wrong number of parameters: " + args.length);
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
    //String[] aa = {"/tmp/foobar", "M2", "M3", "R:"};
    int errCode = ToolRunner.run(new HBaseConfiguration(), new BulkCopy(), args);
    System.exit(errCode);
  }
}
</pre></blockquote>

<h3>Sample running HBase inserts out of Map Task
</h3>

<p>Here's sample code from Andrew Purtell that does HBase insert inside in the mapper rather than via TableReduce.
</p>

<blockquote><pre>
public class MyMap 
  extends TableMap&lt;ImmutableBytesWritable,MapWritable> // or whatever
{
  private HTable table;

  public void configure(JobConf job) {
    super.configure(job);
    try {
      HBaseConfiguration conf = new HBaseConfiguration(job);
      table = new HTable(conf, "mytable");
    } catch (Exception) {
      // can't do anything about this now
    }
  }

  public void map(ImmutableBytesWritable key, RowResult value,
    OutputCollector&lt;ImmutableBytesWritable,MapWritable> output,
    Reporter reporter) throws IOException
  {
    // now we can report an exception opening the table
    if (table == null)
      throw new IOException("could not open mytable");

    // ...

    // commit the result
    BatchUpdate update = new BatchUpdate();
    // ...
    table.commit(update);
  }
}
</pre></blockquote>

<p>This assumes that you do this when setting up your job: JobConf conf = new JobConf(new HBaseConfiguration());
</p>

<p>Or maybe something like this:
<blockquote><pre>
      JobConf conf = new JobConf(new Configuration()); 
      conf.set("hbase.master", myMaster);
</pre></blockquote>
</p>

<h3>Sample MR Bulk Uploader
</h3>
<p>A students/classes example by Naama Kraus.
</p>

<p>Read the class comment below for specification of inputs, prerequisites, etc. In particular, note that the class comment says that this code is for hbase 0.1.x.
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

/*
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
implements Mapper&lt;LongWritable, Text, Text, MapWritable>, Tool {
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
    OutputCollector&lt;Text, MapWritable> output, Reporter r)
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
    public void reduce(Text k, Iterator&lt;MapWritable> v,
      OutputCollector&lt;Text, MapWritable> output, Reporter r)
    throws IOException {
      while (v.hasNext()) {
        r.setStatus("Reducer committing " + k);
        output.collect(k, v.next());
      }
    }
  }
  
  static int printUsage() {
    System.out.println(NAME + " &lt;input> &lt;table_name>");
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
