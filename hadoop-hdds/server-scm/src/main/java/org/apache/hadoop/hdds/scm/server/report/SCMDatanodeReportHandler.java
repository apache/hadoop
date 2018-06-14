/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.server.report;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import java.io.IOException;

/**
 * Datanode Report handlers should implement this interface in order to get
 * call back whenever the report is received from datanode.
 *
 * @param <T> Type of report the handler is interested in.
 */
public abstract class SCMDatanodeReportHandler<T extends GeneratedMessage>
    implements Configurable {

  private Configuration config;
  private StorageContainerManager scm;

  /**
   * Initializes SCMDatanodeReportHandler and associates it with the given
   * StorageContainerManager instance.
   *
   * @param storageContainerManager StorageContainerManager instance to be
   *                                associated with.
   */
  public void init(StorageContainerManager storageContainerManager) {
    this.scm = storageContainerManager;
  }

  /**
   * Returns the associated StorageContainerManager instance. This will be
   * used by the ReportHandler implementations.
   *
   * @return {@link StorageContainerManager}
   */
  protected StorageContainerManager getSCM() {
    return scm;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  /**
   * Processes the report received from datanode. Each ReportHandler
   * implementation is responsible for providing the logic to process the
   * report it's interested in.
   *
   * @param datanodeDetails Datanode Information
   * @param report Report to be processed
   *
   * @throws IOException In case of any exception
   */
  abstract void processReport(DatanodeDetails datanodeDetails, T report)
      throws IOException;
}
