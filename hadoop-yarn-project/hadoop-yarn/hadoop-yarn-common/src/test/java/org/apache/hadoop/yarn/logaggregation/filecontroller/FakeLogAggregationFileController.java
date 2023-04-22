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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class FakeLogAggregationFileController
    extends LogAggregationFileController {

  @Override
  protected void initInternal(Configuration conf) {

  }

  @Override
  public void initializeWriter(LogAggregationFileControllerContext context)
      throws IOException {

  }

  @Override
  public void closeWriter() throws LogAggregationDFSException {

  }

  @Override
  public void write(AggregatedLogFormat.LogKey logKey,
                    AggregatedLogFormat.LogValue logValue) throws IOException {

  }

  @Override
  public void postWrite(LogAggregationFileControllerContext record)
      throws Exception {

  }

  @Override
  public boolean readAggregatedLogs(ContainerLogsRequest logRequest,
                                    OutputStream os) throws IOException {
    return false;
  }

  @Override
  public List<ContainerLogMeta> readAggregatedLogsMeta(
      ContainerLogsRequest logRequest) throws IOException {
    return null;
  }

  @Override
  public void renderAggregatedLogsBlock(HtmlBlock.Block html,
                                        View.ViewContext context) {

  }

  @Override
  public String getApplicationOwner(Path aggregatedLogPath,
                                    ApplicationId appId) throws IOException {
    return null;
  }

  @Override
  public Map<ApplicationAccessType, String> getApplicationAcls(
      Path aggregatedLogPath, ApplicationId appId) throws IOException {
    return null;
  }
}
