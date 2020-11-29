/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.registry.server.dns;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_DNS_SPLIT_REVERSE_ZONE_RANGE;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_DNS_ZONE_MASK;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_DNS_ZONE_SUBNET;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for configuring reverse zones.
 */
public final class ReverseZoneUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReverseZoneUtils.class);

  private static final long POW3 = (long) Math.pow(256, 3);
  private static final long POW2 = (long) Math.pow(256, 2);
  private static final long POW1 = (long) Math.pow(256, 1);

  private ReverseZoneUtils() {
  }

  /**
   * Given a baseIp, range and index, return the network address for the
   * reverse zone.
   *
   * @param baseIp base ip address to perform calculations against.
   * @param range  number of ip addresses per subnet.
   * @param index  the index of the subnet to calculate.
   * @return the calculated ip address.
   * @throws UnknownHostException if an invalid ip is provided.
   */
  protected static String getReverseZoneNetworkAddress(String baseIp, int range,
      int index) throws UnknownHostException {
    if (index < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid index provided, must be positive: %d", index));
    }
    if (range < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid range provided, cannot be negative: %d",
              range));
    }
    return calculateIp(baseIp, range, index);
  }

  /**
   * When splitting the reverse zone, return the number of subnets needed,
   * given the range and netmask.
   *
   * @param conf the Hadoop configuration.
   * @return The number of subnets given the range and netmask.
   */
  protected static long getSubnetCountForReverseZones(Configuration conf) {
    String subnet = conf.get(KEY_DNS_ZONE_SUBNET);
    String mask = conf.get(KEY_DNS_ZONE_MASK);
    String range = conf.get(KEY_DNS_SPLIT_REVERSE_ZONE_RANGE);

    int parsedRange;
    try {
      parsedRange = Integer.parseInt(range);
    } catch (NumberFormatException e) {
      LOG.error("The supplied range is not a valid integer: Supplied range: ",
          range);
      throw e;
    }
    if (parsedRange < 0) {
      String msg = String
          .format("Range cannot be negative: Supplied range: %d", parsedRange);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    long ipCount;
    try {
      SubnetUtils subnetUtils = new SubnetUtils(subnet, mask);
      subnetUtils.setInclusiveHostCount(true);
      ipCount = subnetUtils.getInfo().getAddressCountLong();

    } catch (IllegalArgumentException e) {
      LOG.error("The subnet or mask is invalid: Subnet: {} Mask: {}", subnet,
          mask);
      throw e;
    }

    if (parsedRange == 0) {
      return ipCount;
    }
    return ipCount / parsedRange;
  }

  private static String calculateIp(String baseIp, int range, int index)
      throws UnknownHostException {
    long[] ipParts = splitIp(baseIp);

    long ipNum1 = POW3 * ipParts[0];
    long ipNum2 = POW2 * ipParts[1];
    long ipNum3 = POW1 * ipParts[2];
    long ipNum4 = ipParts[3];
    long ipNum = ipNum1 + ipNum2 + ipNum3 + ipNum4;

    ArrayList<Long> ipPartsOut = new ArrayList<>();
    // First octet
    long temp = ipNum + range * (long) index;
    ipPartsOut.add(0, temp / POW3);

    // Second octet
    temp = temp - ipPartsOut.get(0) * POW3;
    ipPartsOut.add(1, temp / POW2);

    // Third octet
    temp = temp - ipPartsOut.get(1) * POW2;
    ipPartsOut.add(2, temp / POW1);

    // Fourth octet
    temp = temp - ipPartsOut.get(2) * POW1;
    ipPartsOut.add(3, temp);

    return StringUtils.join(ipPartsOut, '.');
  }

  @VisibleForTesting
  protected static long[] splitIp(String baseIp) throws UnknownHostException {
    InetAddress inetAddress;
    try {
      inetAddress = InetAddress.getByName(baseIp);
    } catch (UnknownHostException e) {
      LOG.error("Base IP address is invalid");
      throw e;
    }
    if (inetAddress instanceof Inet6Address) {
      throw new IllegalArgumentException(
          "IPv6 is not yet supported for " + "reverse zones");
    }
    byte[] octets = inetAddress.getAddress();
    if (octets.length != 4) {
      throw new IllegalArgumentException("Base IP address is invalid");
    }
    long[] results = new long[4];
    for (int i = 0; i < octets.length; i++) {
      results[i] = octets[i] & 0xff;
    }
    return results;
  }

}
