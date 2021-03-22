/*
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

package org.apache.hadoop.fs.s3a.audit.impl;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.s3a.audit.AuditManager;
import org.apache.hadoop.fs.s3a.audit.AuditSpan;
import org.apache.hadoop.service.AbstractService;

/**
 * Simple No-op audit manager for use before a real
 * audit chain is set up.
 * It does have the service lifecycle, so do
 * create a unique instance whenever used.
 */
@InterfaceAudience.Private
public class NoopAuditManager extends AbstractService
    implements AuditManager {

  public NoopAuditManager() {
    super("NoopAuditManager");
  }

  @Override
  public AuditSpan getActiveThreadSpan() {
    return NoopSpan.INSTANCE;
  }

  @Override
  public AuditSpan createSpan(final String name,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    return new NoopSpan(name, path1, path2);
  }

  @Override
  public List<RequestHandler2> createRequestHandlers() {
    return new ArrayList<>();
  }

  @Override
  public TransferStateChangeListener createStateChangeListener() {
    return new TransferStateChangeListener() {
      public void transferStateChanged(final Transfer transfer,
          final Transfer.TransferState state) {
      }
    };
  }
}
