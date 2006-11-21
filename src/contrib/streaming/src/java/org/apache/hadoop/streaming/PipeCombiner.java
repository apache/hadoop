/**
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

package org.apache.hadoop.streaming;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.net.URLDecoder;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** A generic Combiner bridge.<br>
 *  To use a Combiner specify -combiner myprogram in hadoopStreaming.
 *  It delegates operations to an external program via stdin and stdout.
 *  In one run of the external program, you can expect all records with
 *  the same key to appear together.
 *  You should not make assumptions about how many times the combiner is
 *  run on your data.
 *  Ideally the combiner and the reducer are the same program, the combiner
 *  partially aggregates the data zero or more times and the reducer
 *  applies the last aggregation pass.
 *  Do not use a Combiner if your reduce logic does not suport
 *  such a multipass aggregation.
 *  @author Michel Tourn
 */
public class PipeCombiner extends PipeReducer {

  String getPipeCommand(JobConf job) {
    String str = job.get("stream.combine.streamprocessor");
    if (str == null) {
      System.err.println("X1003");
      return str;
    }
    try {
      return URLDecoder.decode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
        System.err.println("stream.combine.streamprocessor in jobconf not found");
        return null;
    }
  }

}
