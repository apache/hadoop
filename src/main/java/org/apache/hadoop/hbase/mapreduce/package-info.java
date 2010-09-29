/*
 * Copyright 2010 The Apache Software Foundation
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
<li><a href="#driver">Bundled HBase MapReduce Jobs</a></li>
<li><a href="#sink">HBase as MapReduce job data source and sink</a></li>
<li><a href="#bulk">Bulk Import writing HFiles directly</a></li>
<li><a href="#examples">Example Code</a></li>
</ul>

<h2><a name="classpath">HBase, MapReduce and the CLASSPATH</a></h2>

<p>MapReduce jobs deployed to a MapReduce cluster do not by default have access
to the HBase configuration under <code>$HBASE_CONF_DIR</code> nor to HBase classes.
You could add <code>hbase-site.xml</code> to
<code>$HADOOP_HOME/conf</code> and add
HBase jars to the <code>$HADOOP_HOME/lib</code> and copy these
changes across your cluster (or edit conf/hadoop-env.sh and add them to the
<code>HADOOP_CLASSPATH</code> variable) but this will pollute your
hadoop install with HBase references; its also obnoxious requiring restart of
the hadoop cluster before it'll notice your HBase additions.</p>

<p>As of 0.90.x, HBase will just add its dependency jars to the job
configuration; the dependencies just need to be available on the local
<code>CLASSPATH</code>.  For example, to run the bundled HBase
{@link RowCounter} mapreduce job against a table named <code>usertable</code>,
type:

<blockquote><pre>
$ HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` ${HADOOP_HOME}/bin/hadoop jar ${HBASE_HOME}/hbase-0.90.0.jar rowcounter usertable
</pre></blockquote>

Expand <code>$HBASE_HOME</code> and <code>$HADOOP_HOME</code> in the above
appropriately to suit your local environment.  The content of <code>HADOOP_CLASSPATH</code>
is set to the HBase <code>CLASSPATH</code> via backticking the command
<code>${HBASE_HOME}/bin/hbase classpath</code>.

<p>When the above runs, internally, the HBase jar finds its zookeeper and
<a href="http://code.google.com/p/guava-libraries/">guava</a>,
etc., dependencies on the passed
</code>HADOOP_CLASSPATH</code> and adds the found jars to the mapreduce
job configuration. See the source at
{@link TableMapReduceUtil#addDependencyJars(org.apache.hadoop.mapreduce.Job)}
for how this is done.
</p>
<p>The above may not work if you are running your HBase from its build directory;
i.e. you've done <code>$ mvn test install</code> at
<code>${HBASE_HOME}</code> and you are now
trying to use this build in your mapreduce job.  If you get
<blockquote><pre>java.lang.RuntimeException: java.lang.ClassNotFoundException: org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper
...
</pre></blockquote>
exception thrown, try doing the following:
<blockquote><pre>
$ HADOOP_CLASSPATH=${HBASE_HOME}/target/hbase-0.90.0-SNAPSHOT.jar:`${HBASE_HOME}/bin/hbase classpath` ${HADOOP_HOME}/bin/hadoop jar ${HBASE_HOME}/target/hbase-0.90.0-SNAPSHOT.jar rowcounter usertable
</pre></blockquote>
Notice how we preface the backtick invocation setting
<code>HADOOP_CLASSPATH</code> with reference to the built HBase jar over in
the <code>target</code> directory.
</p>

<h2><a name="driver">Bundled HBase MapReduce Jobs</a></h2>
<p>The HBase jar also serves as a Driver for some bundled mapreduce jobs. To
learn about the bundled mapreduce jobs run:
<blockquote><pre>
$ ${HADOOP_HOME}/bin/hadoop jar ${HBASE_HOME}/hbase-0.90.0-SNAPSHOT.jar
An example program must be given as the first argument.
Valid program names are:
  copytable: Export a table from local cluster to peer cluster
  completebulkload: Complete a bulk data load.
  export: Write table data to HDFS.
  import: Import data written by Export.
  importtsv: Import data in TSV format.
  rowcounter: Count rows in HBase table
</pre></blockquote>

<h2><a name="sink">HBase as MapReduce job data source and sink</a></h2>

<p>HBase can be used as a data source, {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat TableInputFormat},
and data sink, {@link org.apache.hadoop.hbase.mapreduce.TableOutputFormat TableOutputFormat}
or {@link org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat MultiTableOutputFormat},
for MapReduce jobs.
Writing MapReduce jobs that read or write HBase, you'll probably want to subclass
{@link org.apache.hadoop.hbase.mapreduce.TableMapper TableMapper} and/or
{@link org.apache.hadoop.hbase.mapreduce.TableReducer TableReducer}.  See the do-nothing
pass-through classes {@link org.apache.hadoop.hbase.mapreduce.IdentityTableMapper IdentityTableMapper} and
{@link org.apache.hadoop.hbase.mapreduce.IdentityTableReducer IdentityTableReducer} for basic usage.  For a more
involved example, see {@link org.apache.hadoop.hbase.mapreduce.RowCounter}
or review the <code>org.apache.hadoop.hbase.mapreduce.TestTableMapReduce</code> unit test.
</p>

<p>Running mapreduce jobs that have HBase as source or sink, you'll need to
specify source/sink table and column names in your configuration.</p>

<p>Reading from HBase, the TableInputFormat asks HBase for the list of
regions and makes a map-per-region or <code>mapred.map.tasks maps</code>,
whichever is smaller (If your job only has two maps, up mapred.map.tasks
to a number &gt; number of regions). Maps will run on the adjacent TaskTracker
if you are running a TaskTracer and RegionServer per node.
Writing, it may make sense to avoid the reduce step and write yourself back into
HBase from inside your map. You'd do this when your job does not need the sort
and collation that mapreduce does on the map emitted data; on insert,
HBase 'sorts' so there is no point double-sorting (and shuffling data around
your mapreduce cluster) unless you need to. If you do not need the reduce,
you might just have your map emit counts of records processed just so the
framework's report at the end of your job has meaning or set the number of
reduces to zero and use TableOutputFormat. See example code
below. If running the reduce step makes sense in your case, its usually better
to have lots of reducers so load is spread across the HBase cluster.</p>

<p>There is also a new HBase partitioner that will run as many reducers as
currently existing regions.  The
{@link org.apache.hadoop.hbase.mapreduce.HRegionPartitioner} is suitable
when your table is large and your upload is not such that it will greatly
alter the number of existing regions when done; otherwise use the default
partitioner.
</p>

<h2><a name="bulk">Bulk import writing HFiles directly</a></h2>
<p>If importing into a new table, its possible to by-pass the HBase API
and write your content directly to the filesystem properly formatted as
HBase data files (HFiles).  Your import will run faster, perhaps an order of
magnitude faster if not more.  For more on how this mechanism works, see
<a href="http://hbase.apache.org/docs/current/bulk-loads.html">Bulk Loads</code>
documentation.
</p>

<h2><a name="examples">Example Code</a></h2>
<h3>Sample Row Counter</h3>
<p>See {@link org.apache.hadoop.hbase.mapreduce.RowCounter}.  This job uses
{@link org.apache.hadoop.hbase.mapreduce.TableInputFormat TableInputFormat} and
does a count of all rows in specified table.
You should be able to run
it by doing: <code>% ./bin/hadoop jar hbase-X.X.X.jar</code>.  This will invoke
the hbase MapReduce Driver class.  Select 'rowcounter' from the choice of jobs
offered. This will emit rowcouner 'usage'.  Specify tablename, column to count
and output directory.  You may need to add the hbase conf directory to <code>$HADOOP_HOME/conf/hadoop-env.sh#HADOOP_CLASSPATH</code>
so the rowcounter gets pointed at the right hbase cluster (or, build a new jar
with an appropriate hbase-site.xml built into your job jar).
</p>
*/
package org.apache.hadoop.hbase.mapreduce;
