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
<li><a href="#client_example">Example API Usage</a></li>
</ul>

<h2><a name="client_example">Example API Usage</a></h2>

<p>Once you have a running HBase, you probably want a way to hook your application up to it. 
  If your application is in Java, then you should use the Java API. Here's an example of what 
  a simple client might look like.  This example assumes that you've created a table called
  "myTable" with a column family called "myColumnFamily".
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
REPLACE!!!!!!!!
import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

public class MyClient {

  public static void main(String args[]) throws IOException {
    // You need a configuration object to tell the client where to connect.
    // But don't worry, the defaults are pulled from the local config file.
    HBaseConfiguration config = new HBaseConfiguration();

    // This instantiates an HTable object that connects you to the "myTable"
    // table. 
    HTable table = new HTable(config, "myTable");

    // To do any sort of update on a row, you use an instance of the BatchUpdate
    // class. A BatchUpdate takes a row and optionally a timestamp which your
    // updates will affect.  If no timestamp, the server applies current time
    // to the edits.
    BatchUpdate batchUpdate = new BatchUpdate("myRow");

    // The BatchUpdate#put method takes a byte [] (or String) that designates
    // what cell you want to put a value into, and a byte array that is the
    // value you want to store. Note that if you want to store Strings, you
    // have to getBytes() from the String for HBase to store it since HBase is
    // all about byte arrays. The same goes for primitives like ints and longs
    // and user-defined classes - you must find a way to reduce it to bytes.
    // The Bytes class from the hbase util package has utility for going from
    // String to utf-8 bytes and back again and help for other base types.
    batchUpdate.put("myColumnFamily:columnQualifier1", 
      Bytes.toBytes("columnQualifier1 value!"));

    // Deletes are batch operations in HBase as well. 
    batchUpdate.delete("myColumnFamily:cellIWantDeleted");

    // Once you've done all the puts you want, you need to commit the results.
    // The HTable#commit method takes the BatchUpdate instance you've been 
    // building and pushes the batch of changes you made into HBase.
    table.commit(batchUpdate);

    // Now, to retrieve the data we just wrote. The values that come back are
    // Cell instances. A Cell is a combination of the value as a byte array and
    // the timestamp the value was stored with. If you happen to know that the 
    // value contained is a string and want an actual string, then you must 
    // convert it yourself.
    Cell cell = table.get("myRow", "myColumnFamily:columnQualifier1");
    // This could throw a NullPointerException if there was no value at the cell
    // location.
    String valueStr = Bytes.toString(cell.getValue());
    
    // Sometimes, you won't know the row you're looking for. In this case, you
    // use a Scanner. This will give you cursor-like interface to the contents
    // of the table.
    Scanner scanner = 
      // we want to get back only "myColumnFamily:columnQualifier1" when we iterate
      table.getScanner(new String[]{"myColumnFamily:columnQualifier1"});
    
    
    // Scanners return RowResult instances. A RowResult is like the
    // row key and the columns all wrapped up in a single Object. 
    // RowResult#getRow gives you the row key. RowResult also implements 
    // Map, so you can get to your column results easily. 
    
    // Now, for the actual iteration. One way is to use a while loop like so:
    RowResult rowResult = scanner.next();
    
    while (rowResult != null) {
      // print out the row we found and the columns we were looking for
      System.out.println("Found row: " + Bytes.toString(rowResult.getRow()) +
        " with value: " + rowResult.get(Bytes.toBytes("myColumnFamily:columnQualifier1")));
      rowResult = scanner.next();
    }
    
    // The other approach is to use a foreach loop. Scanners are iterable!
    for (RowResult result : scanner) {
      // print out the row we found and the columns we were looking for
      System.out.println("Found row: " + Bytes.toString(rowResult.getRow()) +
        " with value: " + rowResult.get(Bytes.toBytes("myColumnFamily:columnQualifier1")));
    }
    
    // Make sure you close your scanners when you are done!
    // Its probably best to put the iteration into a try/finally with the below
    // inside the finally clause.
    scanner.close();
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
