/**
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
package org.apache.hadoop.yarn.api;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.math.LongRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.proto.SecurityProtos.*;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.*;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.*;
import org.apache.hadoop.yarn.proto.YarnProtos.*;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestPBImplRecords {
  static final Log LOG = LogFactory.getLog(TestPBImplRecords.class);

  private static HashMap<Type, Object> typeValueCache = new HashMap<Type, Object>();
  private static Random rand = new Random();
  private static byte [] bytes = new byte[] {'1', '2', '3', '4'};

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object genTypeValue(Type type) {
    Object ret = typeValueCache.get(type);
    if (ret != null) {
      return ret;
    }
    // only use positive primitive values
    if (type.equals(boolean.class)) {
      return rand.nextBoolean();
    } else if (type.equals(byte.class)) {
      return bytes[rand.nextInt(4)];
    } else if (type.equals(int.class)) {
      return rand.nextInt(1000000);
    } else if (type.equals(long.class)) {
      return Long.valueOf(rand.nextInt(1000000));
    } else if (type.equals(float.class)) {
      return rand.nextFloat();
    } else if (type.equals(double.class)) {
      return rand.nextDouble();
    } else if (type.equals(String.class)) {
      return String.format("%c%c%c",
          'a' + rand.nextInt(26),
          'a' + rand.nextInt(26),
          'a' + rand.nextInt(26));
    } else if (type instanceof Class) {
      Class clazz = (Class)type;
      if (clazz.isArray()) {
        Class compClass = clazz.getComponentType();
        if (compClass != null) {
          ret = Array.newInstance(compClass, 2);
          Array.set(ret, 0, genTypeValue(compClass));
          Array.set(ret, 1, genTypeValue(compClass));
        }
      } else if (clazz.isEnum()) {
        Object [] values = clazz.getEnumConstants();
        ret = values[rand.nextInt(values.length)];
      } else if (clazz.equals(ByteBuffer.class)) {
        // return new ByteBuffer every time
        // to prevent potential side effects
        ByteBuffer buff = ByteBuffer.allocate(4);
        rand.nextBytes(buff.array());
        return buff;
      }
    } else if (type instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType)type;
      Type rawType = pt.getRawType();
      Type [] params = pt.getActualTypeArguments();
      // only support EnumSet<T>, List<T>, Set<T>, Map<K,V>
      if (rawType.equals(EnumSet.class)) {
        if (params[0] instanceof Class) {
          Class c = (Class)(params[0]);
          return EnumSet.allOf(c);
        }
      } if (rawType.equals(List.class)) {
        ret = Lists.newArrayList(genTypeValue(params[0]));
      } else if (rawType.equals(Set.class)) {
        ret = Sets.newHashSet(genTypeValue(params[0]));
      } else if (rawType.equals(Map.class)) {
        Map<Object, Object> map = Maps.newHashMap();
        map.put(genTypeValue(params[0]), genTypeValue(params[1]));
        ret = map;
      }
    }
    if (ret == null) {
      throw new IllegalArgumentException("type " + type + " is not supported");
    }
    typeValueCache.put(type, ret);
    return ret;
  }

  /**
   * this method generate record instance by calling newIntance
   * using reflection, add register the generated value to typeValueCache
   */
  @SuppressWarnings("rawtypes")
  private static Object generateByNewInstance(Class clazz) throws Exception {
    Object ret = typeValueCache.get(clazz);
    if (ret != null) {
      return ret;
    }
    Method newInstance = null;
    Type [] paramTypes = new Type[0];
    // get newInstance method with most parameters
    for (Method m : clazz.getMethods()) {
      int mod = m.getModifiers();
      if (m.getDeclaringClass().equals(clazz) &&
          Modifier.isPublic(mod) &&
          Modifier.isStatic(mod) &&
          m.getName().equals("newInstance")) {
        Type [] pts = m.getGenericParameterTypes();
        if (newInstance == null
            || (pts.length > paramTypes.length)) {
          newInstance = m;
          paramTypes = pts;
        }
      }
    }
    if (newInstance == null) {
      throw new IllegalArgumentException("type " + clazz.getName() +
          " does not have newInstance method");
    }
    Object [] args = new Object[paramTypes.length];
    for (int i=0;i<args.length;i++) {
      args[i] = genTypeValue(paramTypes[i]);
    }
    ret = newInstance.invoke(null, args);
    typeValueCache.put(clazz, ret);
    return ret;
  }

  @BeforeClass
  public static void setup() throws Exception {
    typeValueCache.put(LongRange.class, new LongRange(1000, 2000));
    typeValueCache.put(URL.class, URL.newInstance(
        "http", "localhost", 8080, "file0"));
    typeValueCache.put(SerializedException.class,
        SerializedException.newInstance(new IOException("exception for test")));
    generateByNewInstance(LogAggregationContext.class);
    generateByNewInstance(ApplicationId.class);
    generateByNewInstance(ApplicationAttemptId.class);
    generateByNewInstance(ContainerId.class);
    generateByNewInstance(Resource.class);
    generateByNewInstance(ResourceBlacklistRequest.class);
    generateByNewInstance(ResourceOption.class);
    generateByNewInstance(LocalResource.class);
    generateByNewInstance(Priority.class);
    generateByNewInstance(NodeId.class);
    generateByNewInstance(NodeReport.class);
    generateByNewInstance(Token.class);
    generateByNewInstance(NMToken.class);
    generateByNewInstance(ResourceRequest.class);
    generateByNewInstance(ApplicationAttemptReport.class);
    generateByNewInstance(ApplicationResourceUsageReport.class);
    generateByNewInstance(ApplicationReport.class);
    generateByNewInstance(Container.class);
    generateByNewInstance(ContainerLaunchContext.class);
    generateByNewInstance(ApplicationSubmissionContext.class);
    generateByNewInstance(ContainerReport.class);
    generateByNewInstance(ContainerResourceDecrease.class);
    generateByNewInstance(ContainerResourceIncrease.class);
    generateByNewInstance(ContainerResourceIncreaseRequest.class);
    generateByNewInstance(ContainerStatus.class);
    generateByNewInstance(PreemptionContainer.class);
    generateByNewInstance(PreemptionResourceRequest.class);
    generateByNewInstance(PreemptionContainer.class);
    generateByNewInstance(PreemptionContract.class);
    generateByNewInstance(StrictPreemptionContract.class);
    generateByNewInstance(PreemptionMessage.class);
    generateByNewInstance(StartContainerRequest.class);
    // genByNewInstance does not apply to QueueInfo, cause
    // it is recursive(has sub queues)
    typeValueCache.put(QueueInfo.class, QueueInfo.newInstance("root", 1.0f,
        1.0f, 0.1f, null, null, QueueState.RUNNING, ImmutableSet.of("x", "y"),
        "x && y"));
    generateByNewInstance(QueueUserACLInfo.class);
    generateByNewInstance(YarnClusterMetrics.class);
    // for reservation system
    generateByNewInstance(ReservationId.class);
    generateByNewInstance(ReservationRequest.class);
    generateByNewInstance(ReservationRequests.class);
    generateByNewInstance(ReservationDefinition.class);
  }

  private class GetSetPair {
    public String propertyName;
    public Method getMethod;
    public Method setMethod;
    public Type type;
    public Object testValue;

    @Override
    public String toString() {
      return String.format("{ name=%s, class=%s, value=%s }", propertyName,
          type, testValue);
    }
  }

  private <R> Map<String, GetSetPair> getGetSetPairs(Class<R> recordClass)
      throws Exception {
    Map<String, GetSetPair> ret = new HashMap<String, GetSetPair>();
    Method [] methods = recordClass.getDeclaredMethods();
    // get all get methods
    for (int i = 0; i < methods.length; i++) {
      Method m = methods[i];
      int mod = m.getModifiers();
      if (m.getDeclaringClass().equals(recordClass) &&
          Modifier.isPublic(mod) &&
          (!Modifier.isStatic(mod))) {
        String name = m.getName();
        if (name.equals("getProto")) {
          continue;
        }
        if ((name.length() > 3) && name.startsWith("get") &&
            (m.getParameterTypes().length == 0)) {
          String propertyName = name.substring(3);
          Type valueType = m.getGenericReturnType();
          GetSetPair p = ret.get(propertyName);
          if (p == null) {
            p = new GetSetPair();
            p.propertyName = propertyName;
            p.type = valueType;
            p.getMethod = m;
            ret.put(propertyName, p);
          } else {
            Assert.fail("Multiple get method with same name: " + recordClass
                + p.propertyName);
          }
        }
      }
    }
    // match get methods with set methods
    for (int i = 0; i < methods.length; i++) {
      Method m = methods[i];
      int mod = m.getModifiers();
      if (m.getDeclaringClass().equals(recordClass) &&
          Modifier.isPublic(mod) &&
          (!Modifier.isStatic(mod))) {
        String name = m.getName();
        if (name.startsWith("set") && (m.getParameterTypes().length == 1)) {
          String propertyName = name.substring(3);
          Type valueType = m.getGenericParameterTypes()[0];
          GetSetPair p = ret.get(propertyName);
          if (p != null && p.type.equals(valueType)) {
            p.setMethod = m;
          }
        }
      }
    }
    // exclude incomplete get/set pair, and generate test value
    Iterator<Entry<String, GetSetPair>> itr = ret.entrySet().iterator();
    while (itr.hasNext()) {
      Entry<String, GetSetPair> cur = itr.next();
      GetSetPair gsp = cur.getValue();
      if ((gsp.getMethod == null) ||
          (gsp.setMethod == null)) {
        LOG.info(String.format("Exclude protential property: %s\n", gsp.propertyName));
        itr.remove();
      } else {
        LOG.info(String.format("New property: %s type: %s", gsp.toString(), gsp.type));
        gsp.testValue = genTypeValue(gsp.type);
        LOG.info(String.format(" testValue: %s\n", gsp.testValue));
      }
    }
    return ret;
  }

  private <R, P> void validatePBImplRecord(Class<R> recordClass,
      Class<P> protoClass)
      throws Exception {
    LOG.info(String.format("Validate %s %s\n", recordClass.getName(),
        protoClass.getName()));
    Constructor<R> emptyConstructor = recordClass.getConstructor();
    Constructor<R> pbConstructor = recordClass.getConstructor(protoClass);
    Method getProto = recordClass.getDeclaredMethod("getProto");
    Map<String, GetSetPair> getSetPairs = getGetSetPairs(recordClass);
    R origRecord = emptyConstructor.newInstance();
    for (GetSetPair gsp : getSetPairs.values()) {
      gsp.setMethod.invoke(origRecord, gsp.testValue);
    }
    Object ret = getProto.invoke(origRecord);
    Assert.assertNotNull(recordClass.getName() + "#getProto returns null", ret);
    if (!(protoClass.isAssignableFrom(ret.getClass()))) {
      Assert.fail("Illegal getProto method return type: " + ret.getClass());
    }
    R deserRecord = pbConstructor.newInstance(ret);
    Assert.assertEquals("whole " + recordClass + " records should be equal",
        origRecord, deserRecord);
    for (GetSetPair gsp : getSetPairs.values()) {
      Object origValue = gsp.getMethod.invoke(origRecord);
      Object deserValue = gsp.getMethod.invoke(deserRecord);
      Assert.assertEquals("property " + recordClass.getName() + "#"
          + gsp.propertyName + " should be equal", origValue, deserValue);
    }
  }

  @Test
  public void testAllocateRequestPBImpl() throws Exception {
    validatePBImplRecord(AllocateRequestPBImpl.class, AllocateRequestProto.class);
  }

  @Test
  public void testAllocateResponsePBImpl() throws Exception {
    validatePBImplRecord(AllocateResponsePBImpl.class, AllocateResponseProto.class);
  }

  @Test
  public void testCancelDelegationTokenRequestPBImpl() throws Exception {
    validatePBImplRecord(CancelDelegationTokenRequestPBImpl.class,
        CancelDelegationTokenRequestProto.class);
  }

  @Test
  public void testCancelDelegationTokenResponsePBImpl() throws Exception {
    validatePBImplRecord(CancelDelegationTokenResponsePBImpl.class,
        CancelDelegationTokenResponseProto.class);
  }

  @Test
  public void testFinishApplicationMasterRequestPBImpl() throws Exception {
    validatePBImplRecord(FinishApplicationMasterRequestPBImpl.class,
        FinishApplicationMasterRequestProto.class);
  }

  @Test
  public void testFinishApplicationMasterResponsePBImpl() throws Exception {
    validatePBImplRecord(FinishApplicationMasterResponsePBImpl.class,
        FinishApplicationMasterResponseProto.class);
  }

  @Test
  public void testGetApplicationAttemptReportRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptReportRequestPBImpl.class,
        GetApplicationAttemptReportRequestProto.class);
  }

  @Test
  public void testGetApplicationAttemptReportResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptReportResponsePBImpl.class,
        GetApplicationAttemptReportResponseProto.class);
  }

  @Test
  public void testGetApplicationAttemptsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptsRequestPBImpl.class,
        GetApplicationAttemptsRequestProto.class);
  }

  @Test
  public void testGetApplicationAttemptsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptsResponsePBImpl.class,
        GetApplicationAttemptsResponseProto.class);
  }

  @Test
  public void testGetApplicationReportRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationReportRequestPBImpl.class,
        GetApplicationReportRequestProto.class);
  }

  @Test
  public void testGetApplicationReportResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationReportResponsePBImpl.class,
        GetApplicationReportResponseProto.class);
  }

  @Test
  public void testGetApplicationsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationsRequestPBImpl.class,
        GetApplicationsRequestProto.class);
  }

  @Test
  public void testGetApplicationsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationsResponsePBImpl.class,
        GetApplicationsResponseProto.class);
  }

  @Test
  public void testGetClusterMetricsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterMetricsRequestPBImpl.class,
        GetClusterMetricsRequestProto.class);
  }

  @Test
  public void testGetClusterMetricsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterMetricsResponsePBImpl.class,
        GetClusterMetricsResponseProto.class);
  }

  @Test
  public void testGetClusterNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodesRequestPBImpl.class,
        GetClusterNodesRequestProto.class);
  }

  @Test
  public void testGetClusterNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodesResponsePBImpl.class,
        GetClusterNodesResponseProto.class);
  }

  @Test
  public void testGetContainerReportRequestPBImpl() throws Exception {
    validatePBImplRecord(GetContainerReportRequestPBImpl.class,
        GetContainerReportRequestProto.class);
  }

  @Test
  public void testGetContainerReportResponsePBImpl() throws Exception {
    validatePBImplRecord(GetContainerReportResponsePBImpl.class,
        GetContainerReportResponseProto.class);
  }

  @Test
  public void testGetContainersRequestPBImpl() throws Exception {
    validatePBImplRecord(GetContainersRequestPBImpl.class,
        GetContainersRequestProto.class);
  }

  @Test
  public void testGetContainersResponsePBImpl() throws Exception {
    validatePBImplRecord(GetContainersResponsePBImpl.class,
        GetContainersResponseProto.class);
  }

  @Test
  public void testGetContainerStatusesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetContainerStatusesRequestPBImpl.class,
        GetContainerStatusesRequestProto.class);
  }

  @Test
  public void testGetContainerStatusesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetContainerStatusesResponsePBImpl.class,
        GetContainerStatusesResponseProto.class);
  }

  @Test
  public void testGetDelegationTokenRequestPBImpl() throws Exception {
    validatePBImplRecord(GetDelegationTokenRequestPBImpl.class,
        GetDelegationTokenRequestProto.class);
  }

  @Test
  public void testGetDelegationTokenResponsePBImpl() throws Exception {
    validatePBImplRecord(GetDelegationTokenResponsePBImpl.class,
        GetDelegationTokenResponseProto.class);
  }

  @Test
  public void testGetNewApplicationRequestPBImpl() throws Exception {
    validatePBImplRecord(GetNewApplicationRequestPBImpl.class,
        GetNewApplicationRequestProto.class);
  }

  @Test
  public void testGetNewApplicationResponsePBImpl() throws Exception {
    validatePBImplRecord(GetNewApplicationResponsePBImpl.class,
        GetNewApplicationResponseProto.class);
  }

  @Test
  public void testGetQueueInfoRequestPBImpl() throws Exception {
    validatePBImplRecord(GetQueueInfoRequestPBImpl.class,
        GetQueueInfoRequestProto.class);
  }

  @Test
  public void testGetQueueInfoResponsePBImpl() throws Exception {
    validatePBImplRecord(GetQueueInfoResponsePBImpl.class,
        GetQueueInfoResponseProto.class);
  }

  @Test
  public void testGetQueueUserAclsInfoRequestPBImpl() throws Exception {
    validatePBImplRecord(GetQueueUserAclsInfoRequestPBImpl.class,
        GetQueueUserAclsInfoRequestProto.class);
  }

  @Test
  public void testGetQueueUserAclsInfoResponsePBImpl() throws Exception {
    validatePBImplRecord(GetQueueUserAclsInfoResponsePBImpl.class,
        GetQueueUserAclsInfoResponseProto.class);
  }

  @Test
  public void testKillApplicationRequestPBImpl() throws Exception {
    validatePBImplRecord(KillApplicationRequestPBImpl.class,
        KillApplicationRequestProto.class);
  }

  @Test
  public void testKillApplicationResponsePBImpl() throws Exception {
    validatePBImplRecord(KillApplicationResponsePBImpl.class,
        KillApplicationResponseProto.class);
  }

  @Test
  public void testMoveApplicationAcrossQueuesRequestPBImpl() throws Exception {
    validatePBImplRecord(MoveApplicationAcrossQueuesRequestPBImpl.class,
        MoveApplicationAcrossQueuesRequestProto.class);
  }

  @Test
  public void testMoveApplicationAcrossQueuesResponsePBImpl() throws Exception {
    validatePBImplRecord(MoveApplicationAcrossQueuesResponsePBImpl.class,
        MoveApplicationAcrossQueuesResponseProto.class);
  }

  @Test
  public void testRegisterApplicationMasterRequestPBImpl() throws Exception {
    validatePBImplRecord(RegisterApplicationMasterRequestPBImpl.class,
        RegisterApplicationMasterRequestProto.class);
  }

  @Test
  public void testRegisterApplicationMasterResponsePBImpl() throws Exception {
    validatePBImplRecord(RegisterApplicationMasterResponsePBImpl.class,
        RegisterApplicationMasterResponseProto.class);
  }

  @Test
  public void testRenewDelegationTokenRequestPBImpl() throws Exception {
    validatePBImplRecord(RenewDelegationTokenRequestPBImpl.class,
        RenewDelegationTokenRequestProto.class);
  }

  @Test
  public void testRenewDelegationTokenResponsePBImpl() throws Exception {
    validatePBImplRecord(RenewDelegationTokenResponsePBImpl.class,
        RenewDelegationTokenResponseProto.class);
  }

  @Test
  public void testStartContainerRequestPBImpl() throws Exception {
    validatePBImplRecord(StartContainerRequestPBImpl.class,
        StartContainerRequestProto.class);
  }

  @Test
  public void testStartContainersRequestPBImpl() throws Exception {
    validatePBImplRecord(StartContainersRequestPBImpl.class,
        StartContainersRequestProto.class);
  }

  @Test
  public void testStartContainersResponsePBImpl() throws Exception {
    validatePBImplRecord(StartContainersResponsePBImpl.class,
        StartContainersResponseProto.class);
  }

  @Test
  public void testStopContainersRequestPBImpl() throws Exception {
    validatePBImplRecord(StopContainersRequestPBImpl.class,
        StopContainersRequestProto.class);
  }

  @Test
  public void testStopContainersResponsePBImpl() throws Exception {
    validatePBImplRecord(StopContainersResponsePBImpl.class,
        StopContainersResponseProto.class);
  }

  @Test
  public void testSubmitApplicationRequestPBImpl() throws Exception {
    validatePBImplRecord(SubmitApplicationRequestPBImpl.class,
        SubmitApplicationRequestProto.class);
  }

  @Test
  public void testSubmitApplicationResponsePBImpl() throws Exception {
    validatePBImplRecord(SubmitApplicationResponsePBImpl.class,
        SubmitApplicationResponseProto.class);
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testApplicationAttemptIdPBImpl() throws Exception {
    validatePBImplRecord(ApplicationAttemptIdPBImpl.class,
        ApplicationAttemptIdProto.class);
  }

  @Test
  public void testApplicationAttemptReportPBImpl() throws Exception {
    validatePBImplRecord(ApplicationAttemptReportPBImpl.class,
        ApplicationAttemptReportProto.class);
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testApplicationIdPBImpl() throws Exception {
    validatePBImplRecord(ApplicationIdPBImpl.class, ApplicationIdProto.class);
  }

  @Test
  public void testApplicationReportPBImpl() throws Exception {
    validatePBImplRecord(ApplicationReportPBImpl.class,
        ApplicationReportProto.class);
  }

  @Test
  public void testApplicationResourceUsageReportPBImpl() throws Exception {
    validatePBImplRecord(ApplicationResourceUsageReportPBImpl.class,
        ApplicationResourceUsageReportProto.class);
  }

  @Test
  public void testApplicationSubmissionContextPBImpl() throws Exception {
    validatePBImplRecord(ApplicationSubmissionContextPBImpl.class,
        ApplicationSubmissionContextProto.class);
    
    ApplicationSubmissionContext ctx =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    
    Assert.assertNotNull(ctx.getResource());
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testContainerIdPBImpl() throws Exception {
    validatePBImplRecord(ContainerIdPBImpl.class, ContainerIdProto.class);
  }

  @Test
  public void testContainerLaunchContextPBImpl() throws Exception {
    validatePBImplRecord(ContainerLaunchContextPBImpl.class,
        ContainerLaunchContextProto.class);
  }

  @Test
  public void testContainerPBImpl() throws Exception {
    validatePBImplRecord(ContainerPBImpl.class, ContainerProto.class);
  }

  @Test
  public void testContainerReportPBImpl() throws Exception {
    validatePBImplRecord(ContainerReportPBImpl.class, ContainerReportProto.class);
  }

  @Test
  public void testContainerResourceDecreasePBImpl() throws Exception {
    validatePBImplRecord(ContainerResourceDecreasePBImpl.class,
        ContainerResourceDecreaseProto.class);
  }

  @Test
  public void testContainerResourceIncreasePBImpl() throws Exception {
    validatePBImplRecord(ContainerResourceIncreasePBImpl.class,
        ContainerResourceIncreaseProto.class);
  }

  @Test
  public void testContainerResourceIncreaseRequestPBImpl() throws Exception {
    validatePBImplRecord(ContainerResourceIncreaseRequestPBImpl.class,
        ContainerResourceIncreaseRequestProto.class);
  }

  @Test
  public void testContainerStatusPBImpl() throws Exception {
    validatePBImplRecord(ContainerStatusPBImpl.class, ContainerStatusProto.class);
  }

  @Test
  public void testLocalResourcePBImpl() throws Exception {
    validatePBImplRecord(LocalResourcePBImpl.class, LocalResourceProto.class);
  }

  @Test
  public void testNMTokenPBImpl() throws Exception {
    validatePBImplRecord(NMTokenPBImpl.class, NMTokenProto.class);
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testNodeIdPBImpl() throws Exception {
    validatePBImplRecord(NodeIdPBImpl.class, NodeIdProto.class);
  }

  @Test
  public void testNodeReportPBImpl() throws Exception {
    validatePBImplRecord(NodeReportPBImpl.class, NodeReportProto.class);
  }

  @Test
  public void testPreemptionContainerPBImpl() throws Exception {
    validatePBImplRecord(PreemptionContainerPBImpl.class,
        PreemptionContainerProto.class);
  }

  @Test
  public void testPreemptionContractPBImpl() throws Exception {
    validatePBImplRecord(PreemptionContractPBImpl.class,
        PreemptionContractProto.class);
  }

  @Test
  public void testPreemptionMessagePBImpl() throws Exception {
    validatePBImplRecord(PreemptionMessagePBImpl.class,
        PreemptionMessageProto.class);
  }

  @Test
  public void testPreemptionResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(PreemptionResourceRequestPBImpl.class,
        PreemptionResourceRequestProto.class);
  }

  @Test
  public void testPriorityPBImpl() throws Exception {
    validatePBImplRecord(PriorityPBImpl.class, PriorityProto.class);
  }

  @Test
  public void testQueueInfoPBImpl() throws Exception {
    validatePBImplRecord(QueueInfoPBImpl.class, QueueInfoProto.class);
  }

  @Test
  public void testQueueUserACLInfoPBImpl() throws Exception {
    validatePBImplRecord(QueueUserACLInfoPBImpl.class,
        QueueUserACLInfoProto.class);
  }

  @Test
  public void testResourceBlacklistRequestPBImpl() throws Exception {
    validatePBImplRecord(ResourceBlacklistRequestPBImpl.class,
        ResourceBlacklistRequestProto.class);
  }

  @Test
  @Ignore
  // ignore as ResourceOptionPBImpl is immutable
  public void testResourceOptionPBImpl() throws Exception {
    validatePBImplRecord(ResourceOptionPBImpl.class, ResourceOptionProto.class);
  }

  @Test
  public void testResourcePBImpl() throws Exception {
    validatePBImplRecord(ResourcePBImpl.class, ResourceProto.class);
  }

  @Test
  public void testResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(ResourceRequestPBImpl.class, ResourceRequestProto.class);
  }

  @Test
  public void testSerializedExceptionPBImpl() throws Exception {
    validatePBImplRecord(SerializedExceptionPBImpl.class,
        SerializedExceptionProto.class);
  }

  @Test
  public void testStrictPreemptionContractPBImpl() throws Exception {
    validatePBImplRecord(StrictPreemptionContractPBImpl.class,
        StrictPreemptionContractProto.class);
  }

  @Test
  public void testTokenPBImpl() throws Exception {
    validatePBImplRecord(TokenPBImpl.class, TokenProto.class);
  }

  @Test
  public void testURLPBImpl() throws Exception {
    validatePBImplRecord(URLPBImpl.class, URLProto.class);
  }

  @Test
  public void testYarnClusterMetricsPBImpl() throws Exception {
    validatePBImplRecord(YarnClusterMetricsPBImpl.class,
        YarnClusterMetricsProto.class);
  }

  @Test
  public void testRefreshAdminAclsRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshAdminAclsRequestPBImpl.class,
        RefreshAdminAclsRequestProto.class);
  }

  @Test
  public void testRefreshAdminAclsResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshAdminAclsResponsePBImpl.class,
        RefreshAdminAclsResponseProto.class);
  }

  @Test
  public void testRefreshNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshNodesRequestPBImpl.class,
        RefreshNodesRequestProto.class);
  }

  @Test
  public void testRefreshNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshNodesResponsePBImpl.class,
        RefreshNodesResponseProto.class);
  }

  @Test
  public void testRefreshQueuesRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshQueuesRequestPBImpl.class,
        RefreshQueuesRequestProto.class);
  }

  @Test
  public void testRefreshQueuesResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshQueuesResponsePBImpl.class,
        RefreshQueuesResponseProto.class);
  }

  @Test
  public void testRefreshServiceAclsRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshServiceAclsRequestPBImpl.class,
        RefreshServiceAclsRequestProto.class);
  }

  @Test
  public void testRefreshServiceAclsResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshServiceAclsResponsePBImpl.class,
        RefreshServiceAclsResponseProto.class);
  }

  @Test
  public void testRefreshSuperUserGroupsConfigurationRequestPBImpl()
      throws Exception {
    validatePBImplRecord(RefreshSuperUserGroupsConfigurationRequestPBImpl.class,
        RefreshSuperUserGroupsConfigurationRequestProto.class);
  }

  @Test
  public void testRefreshSuperUserGroupsConfigurationResponsePBImpl()
      throws Exception {
    validatePBImplRecord(RefreshSuperUserGroupsConfigurationResponsePBImpl.class,
        RefreshSuperUserGroupsConfigurationResponseProto.class);
  }

  @Test
  public void testRefreshUserToGroupsMappingsRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshUserToGroupsMappingsRequestPBImpl.class,
        RefreshUserToGroupsMappingsRequestProto.class);
  }

  @Test
  public void testRefreshUserToGroupsMappingsResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshUserToGroupsMappingsResponsePBImpl.class,
        RefreshUserToGroupsMappingsResponseProto.class);
  }

  @Test
  public void testUpdateNodeResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(UpdateNodeResourceRequestPBImpl.class,
        UpdateNodeResourceRequestProto.class);
  }

  @Test
  public void testUpdateNodeResourceResponsePBImpl() throws Exception {
    validatePBImplRecord(UpdateNodeResourceResponsePBImpl.class,
        UpdateNodeResourceResponseProto.class);
  }

  @Test
  public void testReservationSubmissionRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationSubmissionRequestPBImpl.class,
        ReservationSubmissionRequestProto.class);
  }

  @Test
  public void testReservationSubmissionResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationSubmissionResponsePBImpl.class,
        ReservationSubmissionResponseProto.class);
  }

  @Test
  public void testReservationUpdateRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationUpdateRequestPBImpl.class,
        ReservationUpdateRequestProto.class);
  }

  @Test
  public void testReservationUpdateResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationUpdateResponsePBImpl.class,
        ReservationUpdateResponseProto.class);
  }

  @Test
  public void testReservationDeleteRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationDeleteRequestPBImpl.class,
        ReservationDeleteRequestProto.class);
  }

  @Test
  public void testReservationDeleteResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationDeleteResponsePBImpl.class,
        ReservationDeleteResponseProto.class);
  }
  
  @Test
  public void testAddToClusterNodeLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(AddToClusterNodeLabelsRequestPBImpl.class,
        AddToClusterNodeLabelsRequestProto.class);
  }
  
  @Test
  public void testAddToClusterNodeLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(AddToClusterNodeLabelsResponsePBImpl.class,
        AddToClusterNodeLabelsResponseProto.class);
  }
  
  @Test
  public void testRemoveFromClusterNodeLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(RemoveFromClusterNodeLabelsRequestPBImpl.class,
        RemoveFromClusterNodeLabelsRequestProto.class);
  }
  
  @Test
  public void testRemoveFromClusterNodeLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(RemoveFromClusterNodeLabelsResponsePBImpl.class,
        RemoveFromClusterNodeLabelsResponseProto.class);
  }
  
  @Test
  public void testGetClusterNodeLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodeLabelsRequestPBImpl.class,
        GetClusterNodeLabelsRequestProto.class);
  }

  @Test
  public void testGetClusterNodeLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodeLabelsResponsePBImpl.class,
        GetClusterNodeLabelsResponseProto.class);
  }
  
  @Test
  public void testReplaceLabelsOnNodeRequestPBImpl() throws Exception {
    validatePBImplRecord(ReplaceLabelsOnNodeRequestPBImpl.class,
        ReplaceLabelsOnNodeRequestProto.class);
  }

  @Test
  public void testReplaceLabelsOnNodeResponsePBImpl() throws Exception {
    validatePBImplRecord(ReplaceLabelsOnNodeResponsePBImpl.class,
        ReplaceLabelsOnNodeResponseProto.class);
  }
  
  @Test
  public void testGetNodeToLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetNodesToLabelsRequestPBImpl.class,
        GetNodesToLabelsRequestProto.class);
  }

  @Test
  public void testGetNodeToLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetNodesToLabelsResponsePBImpl.class,
        GetNodesToLabelsResponseProto.class);
  }

  @Test
  public void testGetLabelsToNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetLabelsToNodesRequestPBImpl.class,
        GetLabelsToNodesRequestProto.class);
  }

  @Test
  public void testGetLabelsToNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetLabelsToNodesResponsePBImpl.class,
        GetLabelsToNodesResponseProto.class);
  }
}
