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

/** A base class for {@link OutputFormat}.
 * @deprecated Use {@link FileOutputFormat}
 */
@Deprecated
public abstract class OutputFormatBase<K, V> implements OutputFormat<K, V> {

  /**
   * Set whether the output of the job is compressed.
   * @param conf the {@link JobConf} to modify
   * @param compress should the output of the job be compressed?
   */
  public static void setCompressOutput(JobConf conf, boolean compress) {
    conf.setBoolean("mapred.output.compress", compress);
  }
  
  /**
   * Is the job output compressed?
   * @param conf the {@link JobConf} to look in
   * @return <code>true</code> if the job output should be compressed,
   *         <code>false</code> otherwise
   */
  public static boolean getCompressOutput(JobConf conf) {
    return conf.getBoolean("mapred.output.compress", false);
  }
  
  /**
   * Set the {@link CompressionCodec} to be used to compress job outputs.
   * @param conf the {@link JobConf} to modify
   * @param codecClass the {@link CompressionCodec} to be used to
   *                   compress the job outputs
   */
  public static void 
  setOutputCompressorClass(JobConf conf, 
                           Class<? extends CompressionCodec> codecClass) {
    setCompressOutput(conf, true);
    conf.setClass("mapred.output.compression.codec", codecClass, 
                  CompressionCodec.class);
  }
  
  /**
   * Get the {@link CompressionCodec} for compressing the job outputs.
   * @param conf the {@link JobConf} to look in
   * @param defaultValue the {@link CompressionCodec} to return if not set
   * @return the {@link CompressionCodec} to be used to compress the 
   *         job outputs
   * @throws IllegalArgumentException if the class was specified, but not found
   */
  public static Class<? extends CompressionCodec> 
  getOutputCompressorClass(JobConf conf, 
		                       Class<? extends CompressionCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    
    String name = conf.get("mapred.output.compression.codec");
    if (name != null) {
      try {
        codecClass = 
        	conf.getClassByName(name).asSubclass(CompressionCodec.class);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name + 
                                           " was not found.", e);
      }
    }
    return codecClass;
  }
  
  public abstract RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                               JobConf job, String name,
                                               Progressable progress)
    throws IOException;

  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
    throws FileAlreadyExistsException, 
           InvalidJobConfException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = FileOutputFormat.getOutputPath(job);
    if (outDir == null && job.getNumReduceTasks() != 0) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    if (outDir != null && outDir.getFileSystem(job).exists(outDir)) {
      throw new FileAlreadyExistsException("Output directory " + outDir + 
                                           " already exists");
    }
  }

}

