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

<h2>Table of Contents</h2>
<ul>
<li><a href="#overview">Overview</a></li>
<li><a href="#coprocessor">Coprocessor</a></li>
<li><a href="#regionobserver">RegionObserver</a></li>
<li><a href="#commandtarget">Endpoint</a></li>
<li><a href="#load">Coprocessor loading</a></li>
</ul>

<h2><a name="overview">Overview</a></h2>
Coprocessors are code that runs in-process on each region server. Regions
contain references to the coprocessor implementation classes associated
with them. Coprocessor classes can be loaded either from local
jars on the region server's classpath or via the HDFS classloader.
<p>
Multiple types of coprocessors are provided to provide sufficient flexibility
for potential use cases. Right now there are:
<p>
<ul>
<li>Coprocessor: provides region lifecycle management hooks, e.g., region
open/close/split/flush/compact operations.</li>
<li>RegionObserver: provides hook for monitor table operations from
client side, such as table get/put/scan/delete, etc.</li>
<li>Endpoint: provides on demand triggers for any arbitrary function
executed at a region. One use case is column aggregation at region
server. </li>
</ul>

<h2><a name="coprocessor">Coprocessor</a></h2>
A coprocessor is required to
implement <code>Coprocessor</code> interface so that coprocessor framework
can manage it internally.
<p>
Another design goal of this interface is to provide simple features for
making coprocessors useful, while exposing no more internal state or
control actions of the region server than necessary and not exposing them
directly.
<p>
Over the lifecycle of a region, the methods of this interface are invoked
when the corresponding events happen. The master transitions regions
through the following states:
<p>
&nbsp;&nbsp;&nbsp;
unassigned -> pendingOpen -> open -> pendingClose -> closed.
<p>
Coprocessors have opportunity to intercept and handle events in
pendingOpen, open, and pendingClose states.
<p>

<h3>PendingOpen</h3>
<p>
The region server is opening a region to bring it online. Coprocessors
can piggyback or fail this process.
<p>
<ul>
  <li>preOpen, postOpen: Called before and after the region is reported as
 online to the master.</li><p>
</ul>
<p>
<h3>Open</h3>
The region is open on the region server and is processing both client
requests (get, put, scan, etc.) and administrative actions (flush, compact,
split, etc.). Coprocessors can piggyback administrative actions via:
<p>
<ul>
  <li>preFlush, postFlush: Called before and after the memstore is flushed
  into a new store file.</li><p>
  <li>preCompact, postCompact: Called before and after compaction.</li><p>
  <li>preSplit, postSplit: Called after the region is split.</li><p>
</ul>
<p>
<h3>PendingClose</h3>
The region server is closing the region. This can happen as part of normal
operations or may happen when the region server is aborting due to fatal
conditions such as OOME, health check failure, or fatal filesystem
problems. Coprocessors can piggyback this event. If the server is aborting
an indication to this effect will be passed as an argument.
<p>
<ul>
  <li>preClose and postClose: Called before and after the region is
  reported as closed to the master.</li><p>
</ul>
<p>

<h2><a name="regionobserver">RegionObserver</a></h2>
If the coprocessor implements the <code>RegionObserver</code> interface it can
observe and mediate client actions on the region:
<p>
<ul>
  <li>preGet, postGet: Called before and after a client makes a Get
  request.</li><p>
  <li>preExists, postExists: Called before and after the client tests
  for existence using a Get.</li><p>
  <li>prePut and postPut: Called before and after the client stores a value.
  </li><p>
  <li>preDelete and postDelete: Called before and after the client
  deletes a value.</li><p>
  <li>preScannerOpen postScannerOpen: Called before and after the client
  opens a new scanner.</li><p>
  <li>preScannerNext, postScannerNext: Called before and after the client
  asks for the next row on a scanner.</li><p>
  <li>preScannerClose, postScannerClose: Called before and after the client
  closes a scanner.</li><p>
  <li>preCheckAndPut, postCheckAndPut: Called before and after the client
  calls checkAndPut().</li><p>
  <li>preCheckAndDelete, postCheckAndDelete: Called before and after the client
  calls checkAndDelete().</li><p>
</ul>
You can also extend abstract class <code>BaseRegionObserverCoprocessor</code>
which
implements both <code>Coprocessor</code> and <code>RegionObserver</code>.
In addition, it overrides all methods with default behaviors so you don't
have to override all of them.
<p>
Here's an example of what a simple RegionObserver might look like. This
example shows how to implement access control for HBase. This
coprocessor checks user information for a given client request, e.g.,
Get/Put/Delete/Scan by injecting code at certain
<code>RegionObserver</code>
preXXX hooks. If the user is not allowed to access the resource, a
CoprocessorException will be thrown. And the client request will be
denied by receiving this exception.
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
package org.apache.hadoop.hbase.coprocessor;

import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;

// Sample access-control coprocessor. It utilizes RegionObserver
// and intercept preXXX() method to check user privilege for the given table
// and column family.
public class AccessControlCoprocessor extends BaseRegionObserverCoprocessor {
  // @Override
  public Get preGet(CoprocessorEnvironment e, Get get)
      throws CoprocessorException {

    // check permissions..
    if (access_not_allowed)  {
      throw new AccessDeniedException(&quot;User is not allowed to access.&quot;);
    }
    return get;
  }

  // override prePut(), preDelete(), etc.
}
</pre></blockquote>
</div>

<h2><a name="commandtarget">Endpoint</a></h2>
<code>Coprocessor</code> and <code>RegionObserver</code> provide certain hooks
for injecting user code running at each region. The user code will be triggerd
by existing <code>HTable</code> and <code>HBaseAdmin</code> operations at
the certain hook points.
<p>
Through Endpoint and dynamic RPC protocol, you can define your own
interface communicated between client and region server,
i.e., you can create a new method, specify passed parameters and return types
for this new method.
And the new Endpoint methods can be triggered by
calling client side dynamic RPC functions -- <code>HTable.coprocessorExec(...)
</code>.
<p>
To implement a Endpoint, you need to:
<ul>
<li>Extend <code>CoprocessorProtocol</code>: the interface defines
communication protocol for the new Endpoint, and will be
served as the RPC protocol between client and region server.</li>
<li>Extend both <code>BaseEndpointCoprocessor</code> abstract class,
and the above extended <code>CoprocessorProtocol</code> interface:
the actually implemented class running at region server.</li>
</ul>
<p>
Here's an example of performing column aggregation at region server:
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
// A sample protocol for performing aggregation at regions.
public static interface ColumnAggregationProtocol
extends CoprocessorProtocol {
  // Perform aggregation for a given column at the region. The aggregation
  // will include all the rows inside the region. It can be extended to
  // allow passing start and end rows for a fine-grained aggregation.
  public int sum(byte[] family, byte[] qualifier) throws IOException;
}
// Aggregation implementation at a region.
public static class ColumnAggregationEndpoint extends BaseEndpointCoprocessor
implements ColumnAggregationProtocol {
  // @Override
  // Scan the region by the given family and qualifier. Return the aggregation
  // result.
  public int sum(byte[] family, byte[] qualifier)
  throws IOException {
    // aggregate at each region
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    int sumResult = 0;
    // use an internal scanner to perform scanning.
    InternalScanner scanner = getEnvironment().getRegion().getScanner(scan);
    try {
      List&lt;KeyValue&gt; curVals = new ArrayList&lt;KeyValue&gt;();
      boolean done = false;
      do {
        curVals.clear();
        done = scanner.next(curVals);
        KeyValue kv = curVals.get(0);
        sumResult += Bytes.toInt(kv.getValue());
      } while (done);
    } finally {
      scanner.close();
    }
    return sumResult;
  }
}
</pre></blockquote>
</div>
<p>
Client invocations are performed through <code>HTable</code>,
which has the following methods added by dynamic RPC protocol:

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
public &lt;T extends CoprocessorProtocol&gt; T coprocessorProxy(Class&lt;T&gt; protocol, Row row)

public &lt;T extends CoprocessorProtocol, R&gt; void coprocessorExec(
    Class&lt;T&gt; protocol, List&lt;? extends Row&gt; rows,
    BatchCall&lt;T,R&gt; callable, BatchCallback&lt;R&gt; callback)

public &lt;T extends CoprocessorProtocol, R&gt; void coprocessorExec(
    Class&lt;T&gt; protocol, RowRange range,
    BatchCall&lt;T,R&gt; callable, BatchCallback&lt;R&gt; callback)
</pre></blockquote>
</div>

<p>
Here is a client side example of invoking
<code>ColumnAggregationEndpoint</code>:
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
Scan scan;
Map&lt;byte[], Integer&gt; results;

// scan: for all regions
scan = new Scan();
results = table.coprocessorExec(ColumnAggregationProtocol.class, scan,
    new BatchCall&lt;ColumnAggregationProtocol,Integer&gt;() {
      public Integer call(ColumnAggregationProtocol instance) throws IOException{
        return instance.sum(TEST_FAMILY, TEST_QUALIFIER);
      }
    });
int sumResult = 0;
int expectedResult = 0;
for (Map.Entry&lt;byte[], Integer&gt; e : results.entrySet()) {
  sumResult += e.getValue();
}
</pre></blockquote>
</div>
<h2><a name="load">Coprocess loading</a></h2>
A customized coprocessor can be loaded by two different ways, by configuration,
or by <code>HTableDescriptor</code> for a newly created table.
<p>
(Currently we don't really have an on demand coprocessor loading machanism for
opened regions.)
<h3>Load from configuration</h3>
Whenever a region is opened, it will read coprocessor class names from
<code>hbase.coprocessor.region.classes</code> from <code>Configuration</code>.
Coprocessor framework will automatically load the configured classes as
default coprocessors. The classes must be included in the classpath already.

<p>
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
  &lt;property&gt;
    &lt;name&gt;hbase.coprocessor.region.classes&lt;/name&gt;
    &lt;value&gt;org.apache.hadoop.hbase.coprocessor.AccessControllCoprocessor, org.apache.hadoop.hbase.coprocessor.ColumnAggregationProtocol&lt;/value&gt;
    &lt;description&gt;A comma-separated list of Coprocessors that are loaded by
    default. For any override coprocessor method from RegionObservor or
    Coprocessor, these classes' implementation will be called
    in order. After implement your own
    Coprocessor, just put it in HBase's classpath and add the fully
    qualified class name here.
    &lt;/description&gt;
  &lt;/property&gt;
</pre></blockquote>
</div>
<p>
The first defined coprocessor will be assigned
<code>Coprocessor.Priority.SYSTEM</code> as priority. And each following
coprocessor's priority will be incremented by one. Coprocessors are executed
in order according to the natural ordering of the int.

<h3>Load from table attribute</h3>
Coprocessor classes can also be configured at table attribute. The
attribute key must start with "Coprocessor" and values of the form is
&lt;path&gt;:&lt;class&gt;:&lt;priority&gt;, so that the framework can
recognize and load it.
<p>
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
&#39;COPROCESSOR$1&#39; =&gt; &#39;hdfs://localhost:8020/hbase/coprocessors/test.jar:Test:1000&#39;
&#39;COPROCESSOR$2&#39; =&gt; &#39;/hbase/coprocessors/test2.jar:AnotherTest:1001&#39;
</pre></blockquote>
</div>
<p>
&lt;path&gt; must point to a jar, can be on any filesystem supported by the
Hadoop </code>FileSystem</code> object.
<p>
&lt;class&gt; is the coprocessor implementation class. A jar can contain
more than one coprocessor implementation, but only one can be specified
at a time in each table attribute.
<p>
&lt;priority&gt; is an integer. Coprocessors are executed in order according
to the natural ordering of the int. Coprocessors can optionally abort
actions. So typically one would want to put authoritative CPs (security
policy implementations, perhaps) ahead of observers.
<p>
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
  Path path = new Path(fs.getUri() + Path.SEPARATOR +
    "TestClassloading.jar");

  // create a table that references the jar
  HTableDescriptor htd = new HTableDescriptor(getClass().getName());
  htd.addFamily(new HColumnDescriptor("test"));
  htd.setValue("Coprocessor$1",
    path.toString() +
    ":" + classFullName +
    ":" + Coprocessor.Priority.USER);
  HBaseAdmin admin = new HBaseAdmin(this.conf);
  admin.createTable(htd);

<h3>Chain of RegionObservers</h3>
As described above, multiple coprocessors can be loaded at one region at the
same time. In case of RegionObserver, you can have more than one
RegionObservers register to one same hook point, i.e, preGet(), etc.
When a region reach the
hook point, the framework will invoke each registered RegionObserver by the
order of assigned priority.

</pre></blockquote>
</div>

*/
package org.apache.hadoop.hbase.coprocessor;
