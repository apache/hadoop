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

package org.apache.hadoop.fs.s3a.test;

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;

import org.apache.hadoop.fs.s3a.WriteOperationHelper;

/**
 * Stub implementation of writeOperationHelper callbacks.
 */
public class MinimalWriteOperationHelperCallbacks
    implements WriteOperationHelper.WriteOperationHelperCallbacks {

  @Override
  public SelectObjectContentResult selectObjectContent(SelectObjectContentRequest request) {
    return null;
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(
      CompleteMultipartUploadRequest request) {
    return null;
  }

};

