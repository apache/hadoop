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

package org.apache.hadoop.fs.s3a.audit;

import org.apache.hadoop.fs.s3a.CredentialInitializationException;

/**
 * This is in the AWS exception tree so that exceptions raised in the
 * AWS SDK are correctly reported up.
 * It is a subclass of {@link CredentialInitializationException}
 * so that
 * {@code S3AUtils.translateException()} recognizes these exceptions
 * and converts them to AccessDeniedException.
 */
public class AuditFailureException extends CredentialInitializationException {

  public AuditFailureException(final String message, final Throwable t) {
    super(message, t);
  }

  public AuditFailureException(final String message) {
    super(message);
  }

}
