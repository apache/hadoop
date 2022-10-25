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

package org.apache.hadoop.fs.s3a;

import java.util.List;

import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Exception raised in {@link S3AFileSystem#deleteObjects} when
 * one or more of the keys could not be deleted.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MultiObjectDeleteException extends S3Exception {

  private final List<S3Error> errors;

  public MultiObjectDeleteException(List<S3Error> errors) {
    super(builder().message(errors.toString()));
    this.errors = errors;
  }

  public List<S3Error> errors() { return errors; }
}
