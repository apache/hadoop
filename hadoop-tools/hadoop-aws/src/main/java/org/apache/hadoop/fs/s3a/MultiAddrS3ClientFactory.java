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



import org.apache.hadoop.conf.Configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

/**
 * New implementation of multi-endpoint s3a client factory, extends org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
 * */
public class MultiAddrS3ClientFactory extends DefaultS3ClientFactory {

  static AmazonS3 doNewAmazonS3Client(MethodInterceptor callback){
        Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(AmazonS3.class);
    enhancer.setCallback(callback);
    return (AmazonS3) enhancer.create();
  }

  /**
   * Override newAmazonS3Client for new implementation of multi-endpoint s3a client
   * */
  @Override
  protected AmazonS3 newAmazonS3Client(
  AWSCredentialsProvider credentials, ClientConfiguration awsConf) {
      return doNewAmazonS3Client(new MultiAddrAmazonS3Interceptor(
          credentials, awsConf, getConf()));
  }

  private static class MultiAddrAmazonS3Interceptor
    extends AbstractMultiAddrAmazonS3Interceptor{
    private AWSCredentialsProvider credentials;
    private ClientConfiguration awsConf;

    MultiAddrAmazonS3Interceptor(AWSCredentialsProvider credentials,
        ClientConfiguration awsConf, Configuration conf) {
      super(conf);
      this.credentials = credentials;
      this.awsConf = awsConf;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected AmazonS3 doCreateNewClient() {
      //Use AmazonS3Client as implementation
      return new AmazonS3Client(credentials, awsConf);
    }
  }

}
