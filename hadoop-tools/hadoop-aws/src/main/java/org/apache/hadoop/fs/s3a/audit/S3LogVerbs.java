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

/**
 * Various verbs in the S3 logs.
 * https://stackoverflow.com/questions/42707878/amazon-s3-logs-operation-definition
 */
public final class S3LogVerbs {

  private S3LogVerbs() {
  }

  public static final String DELETE = "REST.DELETE.OBJECT";
  public static final String COPY = "REST.COPY.OBJECT";
  public static final String DELETE_BULK = "REST.POST.MULTI_OBJECT_DELETE";
  public static final String DELETE_BULK_ENTRY = "BATCH.DELETE.OBJECT";
  public static final String GET = "REST.GET.OBJECT";
  public static final String HEAD = "REST.HEAD.OBJECT";
  public static final String GET_ACL = "REST.GET.ACL";
  public static final String GET_LOGGING_STATUS = "REST.GET.LOGGING_STATUS";
  public static final String LIST = "REST.GET.BUCKET";
  public static final String MULTIPART_UPLOAD_START = "REST.POST.UPLOADS";
  public static final String MULTIPART_UPLOAD_PART = "REST.PUT.PART";
  public static final String MULTIPART_UPLOAD_COMPLETE = "REST.POST.UPLOAD";
  public static final String MULTIPART_UPLOADS_LIST = "REST.GET.UPLOADS";
  public static final String MULTIPART_UPLOAD_ABORT = "REST.DELETE.UPLOAD";
  public static final String PUT = "REST.PUT.OBJECT";
  public static final String REST_GET_POLICY_STATUS = "REST.GET.POLICY_STATUS";
  public static final String REST_GET_PUBLIC_ACCESS_BLOCK =
      "REST.GET.PUBLIC_ACCESS_BLOCK";
  public static final String REST_GET_TAGGING = "REST.GET.TAGGING";
  public static final String S3_EXPIRE_OBJECT = "S3.EXPIRE.OBJECT";

}
