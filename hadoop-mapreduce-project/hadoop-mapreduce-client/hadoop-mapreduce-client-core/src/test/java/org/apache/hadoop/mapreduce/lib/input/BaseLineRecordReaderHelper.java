/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class BaseLineRecordReaderHelper {

  private final Configuration conf;
  private final Path filePath;
  private final byte[] recordDelimiterBytes;



  public BaseLineRecordReaderHelper(Path filePath, Configuration conf) {
    this.conf = conf;
    this.filePath = filePath;

    conf.setInt(LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);

    String delimiter = conf.get("textinputformat.record.delimiter");
    this.recordDelimiterBytes =
        null != delimiter ? delimiter.getBytes(StandardCharsets.UTF_8) : null;
  }

  public abstract long countRecords(long start, long length) throws IOException;

  public Configuration getConf() {
    return conf;
  }

  public Path getFilePath() {
    return filePath;
  }

  public byte[] getRecordDelimiterBytes() {
    return recordDelimiterBytes;
  }
}
