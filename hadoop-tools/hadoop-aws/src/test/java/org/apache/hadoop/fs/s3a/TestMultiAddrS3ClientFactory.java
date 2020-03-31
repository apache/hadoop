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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class TestMultiAddrS3ClientFactory extends MultiAddrS3ClientFactory {

  private EndpointCallback callback = null;

  @Override
  protected AmazonS3 newAmazonS3Client(
      AWSCredentialsProvider credentials, ClientConfiguration awsConf) {
    AmazonS3 ret = doNewAmazonS3Client(
        new AbstractMultiAddrAmazonS3Interceptor(getConf()) {
      @Override
      protected AmazonS3 doCreateNewClient() {
        return MultiAddrS3ClientFactory.doNewAmazonS3Client(
            new TestAmazonS3Interceptor());
      }
    });
    return ret;
  }

  public void setEndpointCallback(EndpointCallback callback){
    this.callback = callback;
  }

  private class TestAmazonS3Interceptor implements MethodInterceptor {

    private String endpoint = "default";

    @Override
    public Object intercept(Object obj, Method method, Object[] args,
        MethodProxy proxy) throws Throwable {
      if("setEndpoint".equals(method.getName())) {
        endpoint = args[0].toString();
        return null;
      }
      if(callback != null){
        callback.endpointCall(endpoint);
      }
      return null;
    }
  }
}