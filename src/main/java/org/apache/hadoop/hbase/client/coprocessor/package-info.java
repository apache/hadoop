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
Provides client classes for invoking Coprocessor RPC protocols

<p>
<ul>
 <li><a href="#overview">Overview</a></li>
 <li><a href="#usage">Example Usage</a></li>
</ul>
</p>

<h2><a name="overview">Overview</a></h2>
<p>
The coprocessor framework provides a way for custom code to run in place on the
HBase region servers with each of a table's regions.  These client classes
enable applications to communicate with coprocessor instances via custom RPC
protocols.
</p>

<p>
In order to provide a custom RPC protocol to clients, a coprocessor implementation
defines an interface that extends {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol}.
The interface can define any methods that the coprocessor wishes to expose.
Using this protocol, you can communicate with the coprocessor instances via
the {@link org.apache.hadoop.hbase.client.HTable#coprocessorProxy(Class, byte[])} and
{@link org.apache.hadoop.hbase.client.HTable#coprocessorExec(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}
methods.
</p>

<p>
Since {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol} instances are
associated with individual regions within the table, the client RPC calls
must ultimately identify which regions should be used in the <code>CoprocessorProtocol</code>
method invocations.  Since regions are seldom handled directly in client code
and the region names may change over time, the coprocessor RPC calls use row keys
to identify which regions should be used for the method invocations.  Clients
can call <code>CoprocessorProtocol</code> methods against either:
<ul>
 <li><strong>a single region</strong> - calling
   {@link org.apache.hadoop.hbase.client.HTable#coprocessorProxy(Class, byte[])}
   with a single row key.  This returns a dynamic proxy of the <code>CoprocessorProtocol</code>
   interface which uses the region containing the given row key (even if the
   row does not exist) as the RPC endpoint.</li>
 <li><strong>a range of regions</strong> - calling
   {@link org.apache.hadoop.hbase.client.HTable#coprocessorExec(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}
   with a starting row key and an ending row key.  All regions in the table
   from the region containing the start row key to the region containing the end
   row key (inclusive), will we used as the RPC endpoints.</li>
</ul>
</p>

<p><em>Note that the row keys passed as parameters to the <code>HTable</code>
methods are not passed to the <code>CoprocessorProtocol</code> implementations.
They are only used to identify the regions for endpoints of the remote calls.
</em></p>

<p>
The {@link org.apache.hadoop.hbase.client.coprocessor.Batch} class defines two
interfaces used for <code>CoprocessorProtocol</code> invocations against
multiple regions.  Clients implement {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} to
call methods of the actual <code>CoprocessorProtocol</code> instance.  The interface's
<code>call()</code> method will be called once per selected region, passing the
<code>CoprocessorProtocol</code> instance for the region as a parameter.  Clients
can optionally implement {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback}
to be notified of the results from each region invocation as they complete.
The instance's {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
method will be called with the {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
return value from each region.
</p>

<h2><a name="usage">Example usage</a></h2>
<p>
To start with, let's use a fictitious coprocessor, <code>RowCountCoprocessor</code>
that counts the number of rows and key-values in each region where it is running.
For clients to query this information, the coprocessor defines and implements
the following {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol} extension
interface:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
public interface RowCountProtocol extends CoprocessorProtocol {
  long getRowCount();
  long getRowCount(Filter filt);
  long getKeyValueCount();
}
</pre></blockquote></div>

<p>
Now we need a way to access the results that <code>RowCountCoprocessor</code>
is making available.  If we want to find the row count for all regions, we could
use:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
HTable table = new HTable("mytable");
// find row count keyed by region name
Map<byte[],Long> results = table.coprocessorExec(
    RowCountProtocol.class, // the protocol interface we're invoking
    null, null,             // start and end row keys
    new Batch.Call<RowCountProtocol,Long>() {
       public Long call(RowCountProtocol counter) {
         return counter.getRowCount();
       }
     });
</pre></blockquote></div>

<p>
This will return a <code>java.util.Map</code> of the <code>counter.getRowCount()</code>
result for the <code>RowCountCoprocessor</code> instance running in each region
of <code>mytable</code>, keyed by the region name.
</p>

<p>
By implementing {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call}
as an anonymous class, we can invoke <code>RowCountProtocol</code> methods
directly against the {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
method's argument.  Calling {@link org.apache.hadoop.hbase.client.HTable#coprocessorExec(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call)}
will take care of invoking <code>Batch.Call.call()</code> against our anonymous class
with the <code>RowCountCoprocessor</code> instance for each table region.
</p>

<p>
For this simple case, where we only want to obtain the result from a single
<code>CoprocessorProtocol</code> method, there's also a bit of syntactic sugar
we can use to cut down on the amount of code required:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
HTable table = new HTable("mytable");
Batch.Call<RowCountProtocol,Long> call = Batch.forMethod(RowCountProtocol.class, "getRowCount");
Map<byte[],Long> results = table.coprocessorExec(RowCountProtocol.class, null, null, call);
</pre></blockquote></div>

<p>
{@link org.apache.hadoop.hbase.client.coprocessor.Batch#forMethod(Class, String, Object...)}
is a simple factory method that will return a {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call}
instance that will call <code>RowCountProtocol.getRowCount()</code> for us
using reflection.
</p>

<p>
However, if you want to perform additional processing on the results,
implementing {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call}
directly will provide more power and flexibility.  For example, if you would
like to combine row count and key-value count for each region:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
HTable table = new HTable("mytable");
// combine row count and kv count for region
Map<byte[],Pair<Long,Long>> results = table.coprocessorExec(
    RowCountProtocol.class,
    null, null,
    new Batch.Call<RowCountProtocol,Pair<Long,Long>>() {
        public Pair<Long,Long> call(RowCountProtocol counter) {
          return new Pair(counter.getRowCount(), counter.getKeyValueCount());
        }
    });
</pre></blockquote></div>

<p>
Similarly, you could average the number of key-values per row for each region:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
Map<byte[],Double> results = table.coprocessorExec(
    RowCountProtocol.class,
    null, null,
    new Batch.Call<RowCountProtocol,Double>() {
        public Double call(RowCountProtocol counter) {
          return ((double)counter.getKeyValueCount()) / ((double)counter.getRowCount());
        }
    });
</pre></blockquote></div>
*/
package org.apache.hadoop.hbase.client.coprocessor;