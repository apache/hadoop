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
package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;

public class TestSentiment {

  private final static String BASEDIR = System.getProperty("test.build.data",
                                                           "target/test-dir");



  @Test public void testTweetSplits() throws Exception{
    String[] args = new String[2];
    args[0] = "/Users/meetpatel/Downloads/twitterDataset.txt";
    args[1] = "/Users/meetpatel/Downloads/sample_data.txt";
    args[2] = "/Users/meetpatel/Downloads/output";

    //FileReader
    String raw;
    try (BufferedReader fi = new BufferedReader(new FileReader(args[0]))) {
      raw = fi.readLine();
    }
    System.out.println(raw);

    // Split value would be "raw" here.
    String[] splits = SentimentAnalysis.SentimentMapper.splitTweets(raw, ',');
    String user = splits[0];
    String tweet = splits[1];

    System.out.println(user);
    System.out.println(tweet);
  }
}
