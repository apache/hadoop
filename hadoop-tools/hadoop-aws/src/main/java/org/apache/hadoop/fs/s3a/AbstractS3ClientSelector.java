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
import java.util.Arrays;
import java.util.List;
import com.amazonaws.services.s3.AmazonS3;

public abstract class AbstractS3ClientSelector implements S3ClientSelector {
  protected AmazonS3[] s3Arr = new AmazonS3[0];

  public synchronized void setS3Arr(AmazonS3[] s3Arr){
    this.s3Arr = s3Arr;
  }

  public int s3ArrLength(){
    return s3Arr.length;
  }

  public AmazonS3 indexOf(int i){
    return s3Arr[i];
  }

  public List<AmazonS3> s3ClientList(){
    return Arrays.asList(s3Arr);
  }

  public AmazonS3 getCurrS3ByMethod(Method method, Object[] params){
    if(s3Arr.length == 0){
      throw new RuntimeException("selector must at least 1 AmazonS3 client!");
    }
    return doGetCurrS3ByMethod(method, params);
  }

  protected abstract AmazonS3 doGetCurrS3ByMethod(Method method, Object[] params);

}
