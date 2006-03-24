/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.fs.FileSystem;

/** A base class for {@link OutputFormat}. */
public abstract class OutputFormatBase implements OutputFormat {
  public abstract RecordWriter getRecordWriter(FileSystem fs,
                                               JobConf job, String name)
    throws IOException;

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    // Ensure that the output directory is set and not already there
    File outDir = job.getOutputDir();
    if (outDir == null && job.getNumReduceTasks() != 0) {
      throw new IOException("Output directory not set in JobConf.");
    }
    if (outDir != null && fs.exists(outDir)) {
      throw new IOException("Output directory " + outDir + 
                            " already exists.");
    }
  }

}

