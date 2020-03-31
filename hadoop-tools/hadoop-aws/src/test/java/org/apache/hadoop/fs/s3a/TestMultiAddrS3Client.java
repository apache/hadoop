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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.AmazonS3;

public class TestMultiAddrS3Client {
  private AmazonS3 s3;
  private Configuration conf = new Configuration();
  private TestMultiAddrS3ClientFactory factory =
      new TestMultiAddrS3ClientFactory();
  private static final String ENDPOINT_LIST = "aaa,bbb,ccc,ddd,eee";
  private static final String BUCKET_NAME = "Test";

  @Before
  public void setup() throws IOException, URISyntaxException{
    conf.set("fs.s3a.endpoint", ENDPOINT_LIST);
    factory.setConf(conf);
  }

  private static void fillPrimitiveValue(Class<?>[] p, Object[] arr,
      int startIdx){
    for(int i = startIdx;i < p.length;i++){
      Class<?> type = p[i];
      if(int.class == type || long.class == type || short.class == type
          || byte.class == type){
        arr[i] = 0;
      } else if(boolean.class == type) {
        arr[i] = false;
      } else if(char.class == type){
        arr[i] = '\0';
      } else if(float.class == type || double.class == type){
        arr[i] = 0.0;
      }
    }
  }

  /**
   * S3 client route test
   * */
  @Test
  public void testCallAllMethod() throws Exception {
    HashMap<String, Integer> callbackMap = new HashMap<String, Integer>();
    factory.setEndpointCallback(new EndpointCallback() {
      @Override
      public void endpointCall(String endpointName) {
        synchronized(callbackMap){
          Integer ret = callbackMap.get(endpointName);
          if(ret == null) ret = 0;
          callbackMap.put(endpointName, ret+1);
        }
      }
    });
    s3 = factory.createS3Client(new URI("s3a://" + BUCKET_NAME));
    Method[] methods = AmazonS3.class.getDeclaredMethods();
    for(Method m:methods){
      if("setEndpoint".equals(m.getName())) continue;
      if("setRegion".equals(m.getName())) continue;
      if("getRegionName".equals(m.getName())) continue;
      if("getRegion".equals(m.getName())) continue;
      if("createBucket".equals(m.getName())) continue;
      Object[] params = new Object[m.getParameterCount()];
      fillPrimitiveValue(m.getParameterTypes(), params, 0);
      m.invoke(s3, params);
    }
    assertEquals(0, getEndPointCallTimes(callbackMap, "default"));
    for(String ep:ENDPOINT_LIST.split(",")){
      assertNotEquals(0, getEndPointCallTimes(callbackMap, ep));
    }
  }


  private static final String bucketKey = "key";

  /**
   * S3 client route test for path selector
   * */
  @Test
  public void testPathSelector() throws Exception {
    conf.set("fs.s3a.S3ClientSelector.class", PathS3ClientSelector.class.getName());
    HashSet<String> callbackSet = new HashSet<String>();
    factory.setEndpointCallback(new EndpointCallback() {

      @Override
      public void endpointCall(String endpointName) {
        synchronized(callbackSet){
          callbackSet.add(endpointName);
        }
      }
    });
    s3 = factory.createS3Client(new URI("s3a://" + BUCKET_NAME));
    Method[] methods = AmazonS3.class.getDeclaredMethods();
    for(Method m:methods){
      Class<?>[] paramTypes = m.getParameterTypes();
      if(paramTypes.length == 1 &&
          AmazonWebServiceRequest.class.isAssignableFrom(paramTypes[0])){
        Class<?> clazz = paramTypes[0];
        try{
          clazz.getMethod("getBucketName");
          clazz.getMethod("getKey");
        } catch (NoSuchMethodException e){
          continue;
        }
        Constructor<?>[] cons = clazz.getConstructors();
        for(Constructor<?> con:cons){
          Class<?>[] conParamTypes = con.getParameterTypes();
          if(conParamTypes.length >= 2 && conParamTypes[0] == String.class
              && conParamTypes[1] == String.class){
            Object[] conParams = new Object[conParamTypes.length];
            conParams[0] = BUCKET_NAME;
            conParams[1] = bucketKey;
            fillPrimitiveValue(conParamTypes, conParams, 2);
            Object reqObj = con.newInstance(conParams);
            m.invoke(s3, reqObj);
            break;
          }
        }
      }
    }
    assertEquals(1, callbackSet.size());
  }

  private static int getEndPointCallTimes(HashMap<String, Integer> map, String name){
    Integer ret = map.get(name);
    return ret == null?0:ret;
  }

}
