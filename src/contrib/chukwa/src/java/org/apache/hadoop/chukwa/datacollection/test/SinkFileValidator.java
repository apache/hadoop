/*
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

package org.apache.hadoop.chukwa.datacollection.test;

import java.net.URI;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SinkFileValidator {
  
  public static void main(String[] args)
  {
    String fsURL = "hdfs://localhost:9000";
    String fname;
    if(args.length < 1)
    {
      System.out.println("usage:  SinkFileValidator <filename> [filesystem URI] ");
      System.exit(0);
    }
    fname = args[0];
    if(args.length > 1)
      fsURL = args[1];

    Configuration conf = new Configuration();
    try
    {
    FileSystem fs;
    if(fsURL.equals("local"))
      fs = FileSystem.getLocal(conf);
    else
       fs= FileSystem.get(new URI(fsURL), conf);
    SequenceFile.Reader r= new SequenceFile.Reader(fs, new Path(fname), conf);
    System.out.println("key class name is " + r.getKeyClassName());
    System.out.println("value class name is " + r.getValueClassName());
    
    ChukwaArchiveKey key = new ChukwaArchiveKey();
    ChunkImpl evt =  ChunkImpl.getBlankChunk();
    int events = 0;
    while(r.next(key, evt) &&  (events < 5))
    {
      if(!Writable.class.isAssignableFrom(key.getClass()))
        System.out.println("warning: keys aren't writable");
      
      if(!Writable.class.isAssignableFrom(evt.getClass()))
        System.out.println("warning: values aren't writable");
      
      if(evt.getData().length > 1000)
      {
        System.out.println("got event; data: " + new String(evt.getData(), 0, 1000));
        System.out.println("....[truncating]");
      }
      else
        System.out.println("got event; data: " + new String(evt.getData()));
      events ++;
    }
    System.out.println("file looks OK!");
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
    
  }

}
