/*
 * Copyright 2009 The Apache Software Foundation
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
Provides HBase Client

<h2>Table of Contents</h2>
<ul>
 <li><a href="#overview">Overview</a></li>
<li><a href="#client_example">Example API Usage</a></li>
</ul>

 <h2><a name="overview">Overview</a></h2>
 <p>
 </p>
 
<h2><a name="client_example">Example API Usage</a></h2>

<p>Once you have a running HBase, you probably want a way to hook your application up to it. 
  If your application is in Java, then you should use the Java API. Here's an example of what 
  a simple client might look like.  This example assumes that you've created a table called
  "myTable" with a column family called "myColumnFamily".
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class MyLittleHBaseClient {
  public static void main(String[] args) throws IOException {
    // You need a configuration object to tell the client where to connect.
    // When you create a HBaseConfiguration, it reads in whatever you've set
    // into your hbase-site.xml and in hbase-default.xml, as long as these can
    // be found on the CLASSPATH
    HBaseConfiguration config = new HBaseConfiguration();

    // This instantiates an HTable object that connects you to
    // the "myLittleHBaseTable" table. 
    HTable table = new HTable(config, "myLittleHBaseTable");

    // To add to a row, use Put.  A Put constructor takes the name of the row
    // you want to insert into as a byte array.  In HBase, the Bytes class has
    // utility for converting all kinds of java types to byte arrays.  In the
    // below, we are converting the String "myLittleRow" into a byte array to
    // use as a row key for our update. Once you have a Put instance, you can
    // adorn it by setting the names of columns you want to update on the row,
    // the timestamp to use in your update, etc.If no timestamp, the server
    // applies current time to the edits.
    Put p = new Put(Bytes.toBytes("myLittleRow"));

    // To set the value you'd like to update in the row 'myRow', specify the
    // column family, column qualifier, and value of the table cell you'd like
    // to update.  The column family must already exist in your table schema.
    // The qualifier can be anything.  All must be specified as byte arrays as
    // hbase is all about byte arrays.  Lets pretend the table
    // 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
    p.add(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"),
      Bytes.toBytes("Some Value"));

    // Once you've adorned your Put instance with all the updates you want to
    // make, to commit it do the following (The HTable#put method takes the
    // Put instance you've been building and pushes the changes you made into
    // hbase)
    table.put(p);

    // Now, to retrieve the data we just wrote. The values that come back are
    // Result instances. Generally, a Result is an object that will package up
    // the hbase return into the form you find most palatable.
    Get g = new Get(Bytes.toBytes("myLittleRow"));
    Result r = table.get(g);
    byte [] value = r.getValue(Bytes.toBytes("myLittleFamily"),
      Bytes.toBytes("someQualifier"));
    // If we convert the value bytes, we should get back 'Some Value', the
    // value we inserted at this location.
    String valueStr = Bytes.toString(value);
    System.out.println("GET: " + valueStr);

    // Sometimes, you won't know the row you're looking for. In this case, you
    // use a Scanner. This will give you cursor-like interface to the contents
    // of the table.  To set up a Scanner, do like you did above making a Put
    // and a Get, create a Scan.  Adorn it with column names, etc.
    Scan s = new Scan();
    s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
    ResultScanner scanner = table.getScanner(s);
    try {
      // Scanners return Result instances.
      // Now, for the actual iteration. One way is to use a while loop like so:
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        // print out the row we found and the columns we were looking for
        System.out.println("Found row: " + rr);
      }

      // The other approach is to use a foreach loop. Scanners are iterable!
      // for (Result rr : scanner) {
      //   System.out.println("Found row: " + rr);
      // }
    } finally {
      // Make sure you close your scanners when you are done!
      // Thats why we have it inside a try/finally clause
      scanner.close();
    }
  }
}
</pre></blockquote>
</div>

<p>There are many other methods for putting data into and getting data out of 
  HBase, but these examples should get you started. See the HTable javadoc for
  more methods. Additionally, there are methods for managing tables in the 
  HBaseAdmin class.</p>

<p>If your client is NOT Java, then you should consider the Thrift or REST 
  libraries.</p>

<h2><a name="related" >Related Documentation</a></h2>
<ul>
  <li><a href="http://hbase.org">HBase Home Page</a>
  <li><a href="http://wiki.apache.org/hadoop/Hbase">HBase Wiki</a>
  <li><a href="http://hadoop.apache.org/">Hadoop Home Page</a>
</ul>
</pre></code>
</div>

<p>There are many other methods for putting data into and getting data out of 
  HBase, but these examples should get you started. See the HTable javadoc for
  more methods. Additionally, there are methods for managing tables in the 
  HBaseAdmin class.</p>

</body>
</html>
*/
package org.apache.hadoop.hbase.client;
