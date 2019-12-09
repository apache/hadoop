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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to parse job history and configuration files.
 */
class JobHistoryFileParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryFileParser.class);

  private final FileSystem fs;

  public JobHistoryFileParser(FileSystem fs) {
    LOG.info("JobHistoryFileParser created with " + fs);
    this.fs = fs;
  }

  public JobInfo parseHistoryFile(Path path) throws IOException {
    LOG.info("parsing job history file " + path);
    JobHistoryParser parser = new JobHistoryParser(fs, path);
    return parser.parse();
  }

  public Configuration parseConfiguration(Path path) throws IOException {
    LOG.info("parsing job configuration file " + path);
    Configuration conf = new Configuration(false);
    conf.addResource(fs.open(path));
    return conf;
  }
}
