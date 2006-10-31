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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;

/** A base class for {@link OutputFormat}. */
public abstract class OutputFormatBase implements OutputFormat {

  /**
   * Set whether the output of the reduce is compressed
   * @param val the new setting
   */
  public static void setCompressOutput(JobConf conf, boolean val) {
    conf.setBoolean("mapred.output.compress", val);
  }
  
  /**
   * Is the reduce output compressed?
   * @return true, if the output should be compressed
   */
  public static boolean getCompressOutput(JobConf conf) {
    return conf.getBoolean("mapred.output.compress", false);
  }
  
  /**
   * Set the given class as the output compression codec.
   * @param conf the JobConf to modify
   * @param codecClass the CompressionCodec class that will compress the 
   *                   reduce outputs
   */
  public static void setOutputCompressorClass(JobConf conf, Class codecClass) {
    setCompressOutput(conf, true);
    conf.setClass("mapred.output.compression.codec", codecClass, 
                  CompressionCodec.class);
  }
  
  /**
   * Get the codec for compressing the reduce outputs
   * @param conf the Configuration to look in
   * @param defaultValue the value to return if it is not set
   * @return the CompressionCodec class that should be used to compress the 
   *   reduce outputs
   * @throws IllegalArgumentException if the class was specified, but not found
   */
  public static Class getOutputCompressorClass(JobConf conf, 
                                               Class defaultValue) {
    String name = conf.get("mapred.output.compression.codec");
    if (name == null) {
      return defaultValue;
    } else {
      try {
        return conf.getClassByName(name);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name + 
                                           " was not found.", e);
      }
    }
  }
  
  public abstract RecordWriter getRecordWriter(FileSystem fs,
                                               JobConf job, String name,
                                               Progressable progress)
    throws IOException;

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = job.getOutputPath();
    if (outDir == null && job.getNumReduceTasks() != 0) {
      throw new IOException("Output directory not set in JobConf.");
    }
    if (outDir != null && fs.exists(outDir)) {
      throw new IOException("Output directory " + outDir + 
                            " already exists.");
    }
  }

}

