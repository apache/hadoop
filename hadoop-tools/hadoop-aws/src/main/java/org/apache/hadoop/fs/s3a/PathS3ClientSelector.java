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
import java.lang.reflect.Method;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.AmazonS3;

/**
 * Selected by flie path (ensure that the same s3 client will be used for each file)
 * */
public class PathS3ClientSelector extends RandomS3ClientSelector {
  @Override
  protected AmazonS3 doGetCurrS3ByMethod(Method method, Object[] params) {
    if(params.length == 1 && params[0] instanceof AmazonWebServiceRequest){
      Class<?> clazz = params[0].getClass();
      try {
        Method bucketNameMethod = clazz.getMethod("getBucketName");
        Method keyMethod = clazz.getMethod("getKey");
        String url = String.format("%s/%s", bucketNameMethod.invoke(params[0]),
            keyMethod.invoke(params[0]));
        url = url.replace("._COPYING_", "");
        int s3Index = Math.abs(url.hashCode()) % s3Arr.length;
        return s3Arr[s3Index];
      } catch (Exception e) {}
    }
    return super.doGetCurrS3ByMethod(method, params);
  }
}
