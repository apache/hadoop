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
import java.lang.reflect.Method;
import com.amazonaws.services.s3.AmazonS3;


/**
 * S3 client route strategy
 * */
public interface S3ClientSelector {

  /**
   * Get clients which is set by setS3Arr method(Copy or read-only is required for return value).
   * */
  public List<AmazonS3> s3ClientList();

  /**
   * Select client from which is set by setS3Arr method.
   * */
  public AmazonS3 getCurrS3ByMethod(Method method, Object[] params);

  /**
   * Set s3 client reference for selector
   * */
  public void setS3Arr(AmazonS3[] s3Arr);

}
