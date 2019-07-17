/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.ratis.util.TimeDuration;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;

/**
 * Set of Utility functions used in ozone.
 */
@InterfaceAudience.Private
public final class OzoneUtils {

  public static final String ENCODING_NAME = "UTF-8";
  public static final Charset ENCODING = Charset.forName(ENCODING_NAME);

  private OzoneUtils() {
    // Never constructed
  }

  /**
   * Date format that used in ozone. Here the format is thread safe to use.
   */
  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat(
          OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
      format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));

      return format;
    }
  };

  /**
   * Verifies that max key length is a valid value.
   *
   * @param length
   *          The max key length to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyMaxKeyLength(String length)
      throws IllegalArgumentException {
    int maxKey = 0;
    try {
      maxKey = Integer.parseInt(length);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          "Invalid max key length, the vaule should be digital.");
    }

    if (maxKey <= 0) {
      throw new IllegalArgumentException(
          "Invalid max key length, the vaule should be a positive number.");
    }
  }

  /**
   * Returns a random Request ID.
   *
   * Request ID is returned to the client as well as flows through the system
   * facilitating debugging on why a certain request failed.
   *
   * @return String random request ID
   */
  public static String getRequestID() {
    return UUID.randomUUID().toString();
  }

  /**
   * Return host name if possible.
   *
   * @return Host Name or localhost
   */
  public static String getHostName() {
    String host = "localhost";
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // Ignore the error
    }
    return host;
  }

  /**
   * Convert time in millisecond to a human readable format required in ozone.
   * @return a human readable string for the input time
   */
  public static String formatTime(long millis) {
    return DATE_FORMAT.get().format(millis);
  }

  /**
   * Convert time in ozone date format to millisecond.
   * @return time in milliseconds
   */
  public static long formatDate(String date) throws ParseException {
    Preconditions.checkNotNull(date, "Date string should not be null.");
    return DATE_FORMAT.get().parse(date).getTime();
  }

  public static boolean isOzoneEnabled(Configuration conf) {
    return HddsUtils.isHddsEnabled(conf);
  }


  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName)
      throws IllegalArgumentException {

    if (resName == null) {
      throw new IllegalArgumentException("Bucket or Volume name is null");
    }

    if ((resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH) ||
        (resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH)) {
      throw new IllegalArgumentException(
          "Bucket or Volume length is illegal, " +
              "valid length is 3-63 characters");
    }

    if ((resName.charAt(0) == '.') || (resName.charAt(0) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot start with a period or dash");
    }

    if ((resName.charAt(resName.length() - 1) == '.') ||
        (resName.charAt(resName.length() - 1) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot end with a period or dash");
    }

    boolean isIPv4 = true;
    char prev = (char) 0;

    for (int index = 0; index < resName.length(); index++) {
      char currChar = resName.charAt(index);

      if (currChar != '.') {
        isIPv4 = ((currChar >= '0') && (currChar <= '9')) && isIPv4;
      }

      if (currChar > 'A' && currChar < 'Z') {
        throw new IllegalArgumentException(
            "Bucket or Volume name does not support uppercase characters");
      }

      if ((currChar != '.') && (currChar != '-')) {
        if ((currChar < '0') || (currChar > '9' && currChar < 'a') ||
            (currChar > 'z')) {
          throw new IllegalArgumentException("Bucket or Volume name has an " +
              "unsupported character : " +
              currChar);
        }
      }

      if ((prev == '.') && (currChar == '.')) {
        throw new IllegalArgumentException("Bucket or Volume name should not " +
            "have two contiguous periods");
      }

      if ((prev == '-') && (currChar == '.')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have period after dash");
      }

      if ((prev == '.') && (currChar == '-')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have dash after period");
      }
      prev = currChar;
    }

    if (isIPv4) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot be an IPv4 address or all numeric");
    }
  }

  /**
   * Return the TimeDuration configured for the given key. If not configured,
   * return the default value.
   */
  public static TimeDuration getTimeDuration(Configuration conf, String key,
      TimeDuration defaultValue) {
    TimeUnit defaultTimeUnit = defaultValue.getUnit();
    long timeDurationInDefaultUnit = conf.getTimeDuration(key,
        defaultValue.getDuration(), defaultTimeUnit);
    return TimeDuration.valueOf(timeDurationInDefaultUnit, defaultTimeUnit);
  }

  /**
   * Return the time configured for the given key in milliseconds.
   */
  public static long getTimeDurationInMS(Configuration conf, String key,
      TimeDuration defaultValue) {
    return getTimeDuration(conf, key, defaultValue)
        .toLong(TimeUnit.MILLISECONDS);
  }

  /**
   * Helper function to get access acl list for current user.
   *
   * @param userName
   * @param userGroups
   * @return listOfAcls
   * */
  public static List<OzoneAcl> getAclList(String userName,
      List<String> userGroups, ACLType userRights, ACLType groupRights) {

    List<OzoneAcl> listOfAcls = new ArrayList<>();

    // User ACL.
    listOfAcls.add(new OzoneAcl(USER, userName, userRights, ACCESS));
    if(userGroups != null) {
      // Group ACLs of the User.
      userGroups.forEach((group) -> listOfAcls.add(
          new OzoneAcl(GROUP, group, groupRights, ACCESS)));
    }
    return listOfAcls;
  }

  /**
   * Check if acl right requested for given RequestContext exist
   * in provided acl list.
   * Acl validation rules:
   * 1. If user/group has ALL bit set than all user should have all rights.
   * 2. If user/group has NONE bit set than user/group will not have any right.
   * 3. For all other individual rights individual bits should be set.
   *
   * @param acls
   * @param context
   * @return return true if acl list contains right requsted in context.
   * */
  public static boolean checkAclRight(List<OzoneAclInfo> acls,
      RequestContext context) throws OMException {
    String[] userGroups = context.getClientUgi().getGroupNames();
    String userName = context.getClientUgi().getUserName();
    ACLType aclToCheck = context.getAclRights();
    for (OzoneAclInfo a : acls) {
      if(checkAccessInAcl(a, userGroups, userName, aclToCheck)) {
        return true;
      }
    }
    return false;
  }

  private static boolean checkAccessInAcl(OzoneAclInfo a, String[] groups,
      String username, ACLType aclToCheck) {
    BitSet rights = BitSet.valueOf(a.getRights().toByteArray());
    switch (a.getType()) {
    case USER:
      if (a.getName().equals(username)) {
        return checkIfAclBitIsSet(aclToCheck, rights);
      }
      break;
    case GROUP:
      for (String grp : groups) {
         // TODO: Convert ozone acls to proto map format for efficient
        //  acl checks.
        if (a.getName().equals(grp)) {
          return checkIfAclBitIsSet(aclToCheck, rights);
        }
      }
      break;

    default:
      return checkIfAclBitIsSet(aclToCheck, rights);
    }
    return false;
  }

  /**
   * Check if acl right requested for given RequestContext exist
   * in provided acl list.
   * Acl validation rules:
   * 1. If user/group has ALL bit set than all user should have all rights.
   * 2. If user/group has NONE bit set than user/group will not have any right.
   * 3. For all other individual rights individual bits should be set.
   *
   * @param acls
   * @param context
   * @return return true if acl list contains right requsted in context.
   * */
  public static boolean checkAclRights(List<OzoneAcl> acls,
      RequestContext context) throws OMException {
    String[] userGroups = context.getClientUgi().getGroupNames();
    String userName = context.getClientUgi().getUserName();
    ACLType aclToCheck = context.getAclRights();
    // TODO: All ozone types should use one data type for acls. i.e Store
    //  and maintain acls in proto format only.
    for (OzoneAcl a : acls) {
      if (checkAccessInAcl(OzoneAcl.toProtobuf(a), userGroups,
          userName, aclToCheck)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Helper function to check if bit for given acl is set.
   * @param acl
   * @param bitset
   * @return True of acl bit is set else false.
   * */
  public static boolean checkIfAclBitIsSet(ACLType acl, BitSet bitset) {
    if (bitset == null) {
      return false;
    }

    return ((bitset.get(acl.ordinal())
        || bitset.get(ALL.ordinal()))
        && !bitset.get(NONE.ordinal()));
  }

  /**
   * Helper function to find and return all DEFAULT acls in input list with
   * scope changed to ACCESS.
   * @param acls
   *
   * @return list of default Acls.
   * */
  public static Collection<OzoneAclInfo> getDefaultAclsProto(
      List<OzoneAcl> acls) {
    return acls.stream().filter(a -> a.getAclScope() == DEFAULT)
        .map(OzoneAcl::toProtobufWithAccessType).collect(Collectors.toList());
  }

  /**
   * Helper function to find and return all DEFAULT acls in input list with
   * scope changed to ACCESS.
   * @param acls
   *
   * @return list of default Acls.
   * */
  public static Collection<OzoneAcl> getDefaultAcls(List<OzoneAcl> acls) {
    return acls.stream().filter(a -> a.getAclScope() == DEFAULT)
        .collect(Collectors.toList());
  }

}
