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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.protobuf.GeneratedMessageV3;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringLocalResourceMapProto;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.EnumSet;
import java.io.IOException;

/**
 * Common utility methods used by the Router server.
 *
 */
@Private
@Unstable
public final class RouterServerUtil {

  private static final String APPLICATION_ID_PREFIX = "application_";

  private static final String APP_ATTEMPT_ID_PREFIX = "appattempt_";

  private static final String CONTAINER_PREFIX = "container_";

  private static final String EPOCH_PREFIX = "e";

  private static final String RESERVEIDSTR_PREFIX = "reservation_";

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

  /**
   * Throws an exception due to an error.
   *
   * @param errMsg the error message
   * @throws YarnException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowException(String errMsg) throws YarnException {
    LOG.error(errMsg);
    throw new YarnException(errMsg);
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
   * Throws an IOException due to an error.
   *
   * @param t the throwable raised in the called class.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws IOException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowIOException(Throwable t, String errMsgFormat, Object... args)
      throws IOException {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      LOG.error(msg, t);
      throw new IOException(msg, t);
    } else {
      LOG.error(msg);
      throw new IOException(msg);
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
   * Throws an RunTimeException due to an error.
   *
   * @param t the throwable raised in the called class.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @return RuntimeException
   */
  @Public
  @Unstable
  public static RuntimeException logAndReturnRunTimeException(
      Throwable t, String errMsgFormat, Object... args) {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      LOG.error(msg, t);
      return new RuntimeException(msg, t);
    } else {
      LOG.error(msg);
      return new RuntimeException(msg);
    }
  }

  /**
   * Throws an RunTimeException due to an error.
   *
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @return RuntimeException
   */
  @Public
  @Unstable
  public static RuntimeException logAndReturnRunTimeException(
      String errMsgFormat, Object... args) {
    return logAndReturnRunTimeException(null, errMsgFormat, args);
  }

  /**
   * Throws an YarnRuntimeException due to an error.
   *
   * @param t the throwable raised in the called class.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @return YarnRuntimeException
   */
  @Public
  @Unstable
  public static YarnRuntimeException logAndReturnYarnRunTimeException(
      Throwable t, String errMsgFormat, Object... args) {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      LOG.error(msg, t);
      return new YarnRuntimeException(msg, t);
    } else {
      LOG.error(msg);
      return new YarnRuntimeException(msg);
    }
  }

  /**
   * Check applicationId is accurate.
   *
   * We need to ensure that applicationId cannot be empty and
   * can be converted to ApplicationId object normally.
   *
   * @param applicationId applicationId of type string
   * @throws IllegalArgumentException If the format of the applicationId is not accurate,
   * an IllegalArgumentException needs to be thrown.
   */
  @Public
  @Unstable
  public static void validateApplicationId(String applicationId)
      throws IllegalArgumentException {

    // Make Sure applicationId is not empty.
    if (applicationId == null || applicationId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    // Make sure the prefix information of applicationId is accurate.
    if (!applicationId.startsWith(APPLICATION_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid ApplicationId prefix: "
          + applicationId + ". The valid ApplicationId should start with prefix application");
    }

    // Check the split position of the string.
    int pos1 = APPLICATION_ID_PREFIX.length() - 1;
    int pos2 = applicationId.indexOf('_', pos1 + 1);
    if (pos2 < 0) {
      throw new IllegalArgumentException("Invalid ApplicationId: " + applicationId);
    }

    // Confirm that the parsed rmId and appId are numeric types.
    String rmId = applicationId.substring(pos1 + 1, pos2);
    String appId = applicationId.substring(pos2 + 1);
    if(!NumberUtils.isDigits(rmId) || !NumberUtils.isDigits(appId)){
      throw new IllegalArgumentException("Invalid ApplicationId: " + applicationId);
    }
  }

  /**
   * Check appAttemptId is accurate.
   *
   * We need to ensure that appAttemptId cannot be empty and
   * can be converted to ApplicationAttemptId object normally.
   *
   * @param appAttemptId appAttemptId of type string.
   * @throws IllegalArgumentException If the format of the appAttemptId is not accurate,
   * an IllegalArgumentException needs to be thrown.
   */
  @Public
  @Unstable
  public static void validateApplicationAttemptId(String appAttemptId)
      throws IllegalArgumentException {

    // Make Sure appAttemptId is not empty.
    if (appAttemptId == null || appAttemptId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appAttemptId is empty or null.");
    }

    // Make sure the prefix information of appAttemptId is accurate.
    if (!appAttemptId.startsWith(APP_ATTEMPT_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid AppAttemptId prefix: " + appAttemptId);
    }

    // Check the split position of the string.
    int pos1 = APP_ATTEMPT_ID_PREFIX.length() - 1;
    int pos2 = appAttemptId.indexOf('_', pos1 + 1);
    if (pos2 < 0) {
      throw new IllegalArgumentException("Invalid AppAttemptId: " + appAttemptId);
    }
    int pos3 = appAttemptId.indexOf('_', pos2 + 1);
    if (pos3 < 0) {
      throw new IllegalArgumentException("Invalid AppAttemptId: " + appAttemptId);
    }

    // Confirm that the parsed rmId and appId and attemptId are numeric types.
    String rmId = appAttemptId.substring(pos1 + 1, pos2);
    String appId = appAttemptId.substring(pos2 + 1, pos3);
    String attemptId = appAttemptId.substring(pos3 + 1);

    if (!NumberUtils.isDigits(rmId) || !NumberUtils.isDigits(appId)
        || !NumberUtils.isDigits(attemptId)) {
      throw new IllegalArgumentException("Invalid AppAttemptId: " + appAttemptId);
    }
  }

  /**
   * Check containerId is accurate.
   *
   * We need to ensure that containerId cannot be empty and
   * can be converted to ContainerId object normally.
   *
   * @param containerId containerId of type string.
   * @throws IllegalArgumentException If the format of the appAttemptId is not accurate,
   * an IllegalArgumentException needs to be thrown.
   */
  @Public
  @Unstable
  public static void validateContainerId(String containerId)
      throws IllegalArgumentException {

    // Make Sure containerId is not empty.
    if (containerId == null || containerId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the containerId is empty or null.");
    }

    // Make sure the prefix information of containerId is accurate.
    if (!containerId.startsWith(CONTAINER_PREFIX)) {
      throw new IllegalArgumentException("Invalid ContainerId prefix: " + containerId);
    }

    // Check the split position of the string.
    int pos1 = CONTAINER_PREFIX.length() - 1;

    String epoch = "0";
    if (containerId.regionMatches(pos1 + 1, EPOCH_PREFIX, 0, EPOCH_PREFIX.length())) {
      int pos2 = containerId.indexOf('_', pos1 + 1);
      if (pos2 < 0) {
        throw new IllegalArgumentException("Invalid ContainerId: " + containerId);
      }
      String epochStr = containerId.substring(pos1 + 1 + EPOCH_PREFIX.length(), pos2);
      epoch = epochStr;
      // rewind the current position
      pos1 = pos2;
    }

    int pos2 = containerId.indexOf('_', pos1 + 1);
    if (pos2 < 0) {
      throw new IllegalArgumentException("Invalid ContainerId: " + containerId);
    }

    int pos3 = containerId.indexOf('_', pos2 + 1);
    if (pos3 < 0) {
      throw new IllegalArgumentException("Invalid ContainerId: " + containerId);
    }

    int pos4 = containerId.indexOf('_', pos3 + 1);
    if (pos4 < 0) {
      throw new IllegalArgumentException("Invalid ContainerId: " + containerId);
    }

    // Confirm that the parsed appId and clusterTimestamp and attemptId and cid and epoch
    // are numeric types.
    String appId = containerId.substring(pos2 + 1, pos3);
    String clusterTimestamp = containerId.substring(pos1 + 1, pos2);
    String attemptId = containerId.substring(pos3 + 1, pos4);
    String cid = containerId.substring(pos4 + 1);

    if (!NumberUtils.isDigits(appId) || !NumberUtils.isDigits(clusterTimestamp)
        || !NumberUtils.isDigits(attemptId) || !NumberUtils.isDigits(cid)
        || !NumberUtils.isDigits(epoch)) {
      throw new IllegalArgumentException("Invalid ContainerId: " + containerId);
    }
  }

  public static boolean isAllowedDelegationTokenOp() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      return EnumSet.of(UserGroupInformation.AuthenticationMethod.KERBEROS,
          UserGroupInformation.AuthenticationMethod.KERBEROS_SSL,
          UserGroupInformation.AuthenticationMethod.CERTIFICATE)
          .contains(UserGroupInformation.getCurrentUser()
          .getRealAuthenticationMethod());
    } else {
      return true;
    }
  }

  public static String getRenewerForToken(Token<RMDelegationTokenIdentifier> token)
      throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    // we can always renew our own tokens
    return loginUser.getUserName().equals(user.getUserName())
        ? token.decodeIdentifier().getRenewer().toString() : user.getShortUserName();
  }

  /**
   * Set User information.
   *
   * If the username is empty, we will use the Yarn Router user directly.
   * Do not create a proxy user if userName matches the userName on current UGI.
   *
   * @param userName userName.
   * @return UserGroupInformation.
   */
  public static UserGroupInformation setupUser(final String userName) {
    UserGroupInformation user = null;
    try {
      // If userName is empty, we will return UserGroupInformation.getCurrentUser.
      // Do not create a proxy user if user name matches the user name on
      // current UGI
      if (userName == null || userName.trim().isEmpty()) {
        user = UserGroupInformation.getCurrentUser();
      } else if (UserGroupInformation.isSecurityEnabled()) {
        user = UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());
      } else if (userName.equalsIgnoreCase(UserGroupInformation.getCurrentUser().getUserName())) {
        user = UserGroupInformation.getCurrentUser();
      } else {
        user = UserGroupInformation.createProxyUser(userName,
            UserGroupInformation.getCurrentUser());
      }
      return user;
    } catch (IOException e) {
      throw RouterServerUtil.logAndReturnYarnRunTimeException(e,
          "Error while creating Router Service for user : %s.", user);
    }
  }

  /**
   * Check reservationId is accurate.
   *
   * We need to ensure that reservationId cannot be empty and
   * can be converted to ReservationId object normally.
   *
   * @param reservationId reservationId.
   * @throws IllegalArgumentException If the format of the reservationId is not accurate,
   * an IllegalArgumentException needs to be thrown.
   */
  @Public
  @Unstable
  public static void validateReservationId(String reservationId) throws IllegalArgumentException {

    if (reservationId == null || reservationId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the reservationId is empty or null.");
    }

    if (!reservationId.startsWith(RESERVEIDSTR_PREFIX)) {
      throw new IllegalArgumentException("Invalid ReservationId: " + reservationId);
    }

    String[] resFields = reservationId.split("_");
    if (resFields.length != 3) {
      throw new IllegalArgumentException("Invalid ReservationId: " + reservationId);
    }

    String clusterTimestamp = resFields[1];
    String id = resFields[2];
    if (!NumberUtils.isDigits(id) || !NumberUtils.isDigits(clusterTimestamp)) {
      throw new IllegalArgumentException("Invalid ReservationId: " + reservationId);
    }
  }

  /**
   * Convert ReservationDefinitionInfo to ReservationDefinition.
   *
   * @param definitionInfo ReservationDefinitionInfo Object.
   * @return ReservationDefinition.
   */
  public static ReservationDefinition convertReservationDefinition(
      ReservationDefinitionInfo definitionInfo) {
    if (definitionInfo == null || definitionInfo.getReservationRequests() == null
        || definitionInfo.getReservationRequests().getReservationRequest() == null
        || definitionInfo.getReservationRequests().getReservationRequest().isEmpty()) {
      throw new RuntimeException("definitionInfo Or ReservationRequests is Null.");
    }

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
    ReservationRequests reservationRequests = ReservationRequests.newInstance(
        reservationRequestList, reservationRequestInterpreter);

    ReservationDefinition definition = ReservationDefinition.newInstance(
        arrival, deadline, reservationRequests, name, recurrenceExpression, priority);

    return definition;
  }

  /**
   * Checks if the ApplicationSubmissionContext submitted with the application
   * is valid.
   *
   * Current checks:
   * - if its size is within limits.
   *
   * @param appContext the app context to check.
   * @throws IOException if an IO error occurred.
   * @throws YarnException yarn exception.
   */
  @Public
  @Unstable
  public static void checkAppSubmissionContext(ApplicationSubmissionContextPBImpl appContext,
      Configuration conf) throws IOException, YarnException {
    // Prevents DoS over the ApplicationClientProtocol by checking the context
    // the application was submitted with for any excessively large fields.
    double bytesOfMaxAscSize = conf.getStorageSize(
        YarnConfiguration.ROUTER_ASC_INTERCEPTOR_MAX_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_ASC_INTERCEPTOR_MAX_SIZE, StorageUnit.BYTES);
    if (appContext != null) {
      int bytesOfSerializedSize = appContext.getProto().getSerializedSize();
      if (bytesOfSerializedSize >= bytesOfMaxAscSize) {
        logContainerLaunchContext(appContext);
        String applicationId = appContext.getApplicationId().toString();
        String limit = StringUtils.byteDesc((long) bytesOfMaxAscSize);
        String appContentSize = StringUtils.byteDesc(bytesOfSerializedSize);
        String errMsg = String.format(
            "The size of the ApplicationSubmissionContext of the application %s is " +
            "above the limit %s, size = %s.", applicationId, limit, appContentSize);
        LOG.error(errMsg);
        throw new YarnException(errMsg);
      }
    }
  }

  /**
   * Private helper for checkAppSubmissionContext that logs the fields in the
   * context for debugging.
   *
   * @param appContext the app context.
   * @throws IOException if an IO error occurred.
   */
  @Private
  @Unstable
  private static void logContainerLaunchContext(ApplicationSubmissionContextPBImpl appContext)
      throws IOException {
    if (appContext == null || appContext.getAMContainerSpec() == null ||
        !(appContext.getAMContainerSpec() instanceof ContainerLaunchContextPBImpl)) {
      return;
    }

    ContainerLaunchContext launchContext = appContext.getAMContainerSpec();
    ContainerLaunchContextPBImpl clc = (ContainerLaunchContextPBImpl) launchContext;
    LOG.warn("ContainerLaunchContext size: {}.", clc.getProto().getSerializedSize());

    // ContainerLaunchContext contains:
    // 1) Map<String, LocalResource> localResources,
    List<StringLocalResourceMapProto> lrs = clc.getProto().getLocalResourcesList();
    logContainerLaunchContext("LocalResource size: {}. Length: {}.", lrs);

    // 2) Map<String, String> environment, List<String> commands,
    List<StringStringMapProto> envs = clc.getProto().getEnvironmentList();
    logContainerLaunchContext("Environment size: {}. Length: {}.", envs);

    List<String> cmds = clc.getCommands();
    if (CollectionUtils.isNotEmpty(cmds)) {
      LOG.warn("Commands size: {}. Length: {}.", cmds.size(), serialize(cmds).length);
    }

    // 3) Map<String, ByteBuffer> serviceData,
    List<StringBytesMapProto> serviceData = clc.getProto().getServiceDataList();
    logContainerLaunchContext("ServiceData size: {}. Length: {}.", serviceData);

    // 4) Map<ApplicationAccessType, String> acls
    List<ApplicationACLMapProto> acls = clc.getProto().getApplicationACLsList();
    logContainerLaunchContext("ACLs size: {}. Length: {}.", acls);
  }

  /**
   * Log ContainerLaunchContext Data SerializedSize.
   *
   * @param format format of logging.
   * @param lists data list.
   * @param <R> generic type R.
   */
  private static <R extends GeneratedMessageV3> void logContainerLaunchContext(String format,
      List<R> lists) {
    if (CollectionUtils.isNotEmpty(lists)) {
      int sumLength = 0;
      for (R item : lists) {
        sumLength += item.getSerializedSize();
      }
      LOG.warn(format, lists.size(), sumLength);
    }
  }

  /**
   * Serialize an object in ByteArray.
   *
   * @return obj ByteArray.
   * @throws IOException if an IO error occurred.
   */
  @Private
  @Unstable
  private static byte[] serialize(Object obj) throws IOException {
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }

  /**
   * Get trimmed version of ApplicationSubmissionContext to be saved to
   * Federation State Store.
   *
   * @param actualContext actual ApplicationSubmissionContext.
   * @return trimmed ApplicationSubmissionContext.
   */
  @Private
  @Unstable
  public static ApplicationSubmissionContext getTrimmedAppSubmissionContext(
      ApplicationSubmissionContext actualContext) {
    if (actualContext == null) {
      return null;
    }

    // Set Basic information
    ApplicationSubmissionContext trimmedContext =
        Records.newRecord(ApplicationSubmissionContext.class);
    trimmedContext.setApplicationId(actualContext.getApplicationId());
    trimmedContext.setApplicationName(actualContext.getApplicationName());
    trimmedContext.setQueue(actualContext.getQueue());
    trimmedContext.setPriority(actualContext.getPriority());
    trimmedContext.setApplicationType(actualContext.getApplicationType());
    trimmedContext.setNodeLabelExpression(actualContext.getNodeLabelExpression());
    trimmedContext.setLogAggregationContext(actualContext.getLogAggregationContext());
    trimmedContext.setApplicationTags(actualContext.getApplicationTags());
    trimmedContext.setApplicationSchedulingPropertiesMap(
        actualContext.getApplicationSchedulingPropertiesMap());
    trimmedContext.setKeepContainersAcrossApplicationAttempts(
        actualContext.getKeepContainersAcrossApplicationAttempts());
    trimmedContext.setApplicationTimeouts(actualContext.getApplicationTimeouts());

    return trimmedContext;
  }

  public static boolean isRouterWebProxyEnable(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.ROUTER_WEBAPP_PROXY_ENABLE,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_PROXY_ENABLE);
  }
}