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

package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.io.IOException;

/**
 * Common utility methods used by the Router server.
 *
 */
@Private
@Unstable
public final class RouterServerUtil {

  /** Disable constructor. */
  private RouterServerUtil() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(RouterServerUtil.class);

  /**
   * Throws an exception due to an error.
   *
   * @param t the throwable raised in the called class.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws YarnException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowException(Throwable t, String errMsgFormat, Object... args)
      throws YarnException {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      LOG.error(msg, t);
      throw new YarnException(msg, t);
    } else {
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  /**
   * Throws an exception due to an error.
   *
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws YarnException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowException(String errMsg, Throwable t)
      throws YarnException {
    if (t != null) {
      LOG.error(errMsg, t);
      throw new YarnException(errMsg, t);
    } else {
      LOG.error(errMsg);
      throw new YarnException(errMsg);
    }
  }

  public static <R> R createRequestInterceptorChain(Configuration conf, String pipeLineClassName,
      String interceptorClassName, Class<R> clazz) {

    List<String> interceptorClassNames = getInterceptorClassNames(conf,
        pipeLineClassName, interceptorClassName);

    R pipeline = null;
    R current = null;

    for (String className : interceptorClassNames) {
      try {
        Class<?> interceptorClass = conf.getClassByName(className);
        if (clazz.isAssignableFrom(interceptorClass)) {
          Object interceptorInstance = ReflectionUtils.newInstance(interceptorClass, conf);
          if (pipeline == null) {
            pipeline = clazz.cast(interceptorInstance);
            current = clazz.cast(interceptorInstance);
            continue;
          } else {
            Method method = clazz.getMethod("setNextInterceptor", clazz);
            method.invoke(current, interceptorInstance);
            current = clazz.cast(interceptorInstance);
          }
        } else {
          LOG.error("Class: {} not instance of {}.", className, clazz.getCanonicalName());
          throw new YarnRuntimeException("Class: " + className + " not instance of "
              + clazz.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        LOG.error("Could not instantiate RequestInterceptor: {}", className, e);
        throw new YarnRuntimeException("Could not instantiate RequestInterceptor: " + className, e);
      } catch (InvocationTargetException e) {
        LOG.error("RequestInterceptor {} call setNextInterceptor error.", className, e);
        throw new YarnRuntimeException("RequestInterceptor " + className
            + " call setNextInterceptor error.", e);
      } catch (NoSuchMethodException e) {
        LOG.error("RequestInterceptor {} does not contain the method setNextInterceptor.",
            className);
        throw new YarnRuntimeException("RequestInterceptor " + className +
            " does not contain the method setNextInterceptor.", e);
      } catch (IllegalAccessException e) {
        LOG.error("RequestInterceptor {} call the method setNextInterceptor " +
            "does not have access.", className);
        throw new YarnRuntimeException("RequestInterceptor "
            + className + " call the method setNextInterceptor does not have access.", e);
      }
    }

    if (pipeline == null) {
      throw new YarnRuntimeException(
          "RequestInterceptor pipeline is not configured in the system.");
    }

    return pipeline;
  }

  private static List<String> getInterceptorClassNames(Configuration conf,
      String pipeLineClass, String interceptorClass) {
    String configuredInterceptorClassNames = conf.get(pipeLineClass, interceptorClass);
    List<String> interceptorClassNames = new ArrayList<>();
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }
    return interceptorClassNames;
  }

  /**
   * Throws an IOException due to an error.
   *
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws IOException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowIOException(String errMsg, Throwable t)
      throws IOException {
    if (t != null) {
      LOG.error(errMsg, t);
      throw new IOException(errMsg, t);
    } else {
      LOG.error(errMsg);
      throw new IOException(errMsg);
    }
  }

  /**
   * Throws an RunTimeException due to an error.
   *
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws RuntimeException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowRunTimeException(String errMsg, Throwable t)
      throws RuntimeException {
    if (t != null) {
      LOG.error(errMsg, t);
      throw new RuntimeException(errMsg, t);
    } else {
      LOG.error(errMsg);
      throw new RuntimeException(errMsg);
    }
  }

  /**
   * Throws an RunTimeException due to an error.
   *
   * @param t the throwable raised in the called class.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws RuntimeException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowRunTimeException(Throwable t, String errMsgFormat, Object... args)
      throws RuntimeException {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      LOG.error(msg, t);
      throw new RuntimeException(msg, t);
    } else {
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  /**
   * Save Reservation And HomeSubCluster Mapping.
   *
   * @param federationFacade federation facade
   * @param reservationId reservationId
   * @param homeSubCluster homeSubCluster
   * @throws YarnException on failure
   */
  public static void addReservationHomeSubCluster(FederationStateStoreFacade federationFacade,
      ReservationId reservationId, ReservationHomeSubCluster homeSubCluster) throws YarnException {
    try {
      // persist the mapping of reservationId and the subClusterId which has
      // been selected as its home
      federationFacade.addReservationHomeSubCluster(homeSubCluster);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowException(e,
          "Unable to insert the ReservationId %s into the FederationStateStore.", reservationId);
    }
  }

  /**
   * Update Reservation And HomeSubCluster Mapping.
   *
   * @param federationFacade federation facade
   * @param subClusterId subClusterId
   * @param reservationId reservationId
   * @param homeSubCluster homeSubCluster
   * @throws YarnException on failure
   */
  public static void updateReservationHomeSubCluster(FederationStateStoreFacade federationFacade,
      SubClusterId subClusterId, ReservationId reservationId,
      ReservationHomeSubCluster homeSubCluster) throws YarnException {
    try {
      // update the mapping of reservationId and the home subClusterId to
      // the new subClusterId we have selected
      federationFacade.updateReservationHomeSubCluster(homeSubCluster);
    } catch (YarnException e) {
      SubClusterId subClusterIdInStateStore =
          federationFacade.getReservationHomeSubCluster(reservationId);
      if (subClusterId == subClusterIdInStateStore) {
        LOG.info("Reservation {} already submitted on SubCluster {}.", reservationId, subClusterId);
      } else {
        RouterServerUtil.logAndThrowException(e,
            "Unable to update the ReservationId %s into the FederationStateStore.", reservationId);
      }
    }
  }

  /**
   * Exists ReservationHomeSubCluster Mapping.
   *
   * @param federationFacade federation facade
   * @param reservationId reservationId
   * @return true - exist, false - not exist
   */
  public static Boolean existsReservationHomeSubCluster(FederationStateStoreFacade federationFacade,
      ReservationId reservationId) {
    try {
      SubClusterId subClusterId = federationFacade.getReservationHomeSubCluster(reservationId);
      if (subClusterId != null) {
        return true;
      }
    } catch (YarnException e) {
      LOG.warn("get homeSubCluster by reservationId = {} error.", reservationId, e);
    }
    return false;
  }

  public static ReservationDefinition convertReservationDefinition(
      ReservationDefinitionInfo definitionInfo) {

    // basic variable
    long arrival = definitionInfo.getArrival();
    long deadline = definitionInfo.getDeadline();

    // ReservationRequests reservationRequests
    String name = definitionInfo.getReservationName();
    String recurrenceExpression = definitionInfo.getRecurrenceExpression();
    Priority priority = Priority.newInstance(definitionInfo.getPriority());

    // reservation requests info
    List<ReservationRequest> reservationRequestList = new ArrayList<>();

    ReservationRequestsInfo reservationRequestsInfo = definitionInfo.getReservationRequests();

    List<ReservationRequestInfo> reservationRequestInfos =
        reservationRequestsInfo.getReservationRequest();

    for (ReservationRequestInfo resRequestInfo : reservationRequestInfos) {
      ResourceInfo resourceInfo = resRequestInfo.getCapability();
      Resource capability =
          Resource.newInstance(resourceInfo.getMemorySize(), resourceInfo.getvCores());
      ReservationRequest reservationRequest = ReservationRequest.newInstance(capability,
          resRequestInfo.getNumContainers(), resRequestInfo.getMinConcurrency(),
          resRequestInfo.getDuration());
      reservationRequestList.add(reservationRequest);
    }

    ReservationRequestInterpreter[] values = ReservationRequestInterpreter.values();
    ReservationRequestInterpreter reservationRequestInterpreter =
        values[reservationRequestsInfo.getReservationRequestsInterpreter()];
    ReservationRequests reservationRequests =
        ReservationRequests.newInstance(reservationRequestList, reservationRequestInterpreter);

    ReservationDefinition definition =
        ReservationDefinition.newInstance(
            arrival, deadline, reservationRequests, name, recurrenceExpression, priority);

    return definition;
  }
}