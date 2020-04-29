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
package org.apache.hadoop.tools.dynamometer.workloadgenerator.audit;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * A simple {@link TextInputFormat} that disables splitting of files. This is
 * the {@link org.apache.hadoop.mapreduce.InputFormat} used by
 * {@link AuditReplayMapper}.
 */
public class NoSplitTextInputFormat extends TextInputFormat {

  @Override
  public List<FileStatus> listStatus(JobContext context) throws IOException {
    context.getConfiguration().set(FileInputFormat.INPUT_DIR,
        context.getConfiguration().get(AuditReplayMapper.INPUT_PATH_KEY));
    return super.listStatus(context);
  }

  @Override
  public boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}
