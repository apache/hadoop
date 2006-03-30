/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.UTF8;


/** A {@link Mapper} that maps text values into <token,freq> pairs.  Uses
 * {@link StringTokenizer} to break text into tokens. */
public class TokenCountMapper extends MapReduceBase implements Mapper {

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    // get input text
    String text = ((UTF8)value).toString();       // value is line of text

    // tokenize the value
    StringTokenizer st = new StringTokenizer(text);
    while (st.hasMoreTokens()) {
      // output <token,1> pairs
      output.collect(new UTF8(st.nextToken()), new LongWritable(1));
    }  
  }
  
}
