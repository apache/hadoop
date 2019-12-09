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
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class Count extends FsCommand {
  /**
   * Register the names for the count command
   * @param factory the command factory that will instantiate this class
   */
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Count.class, "-count");
  }

  private static final String OPTION_QUOTA = "q";
  private static final String OPTION_HUMAN = "h";
  private static final String OPTION_HEADER = "v";
  private static final String OPTION_TYPE = "t";
  // exclude snapshots from calculation. Only work on default columns.
  private static final String OPTION_EXCLUDE_SNAPSHOT = "x";
  //return the quota, namespace count and disk space usage.
  private static final String OPTION_QUOTA_AND_USAGE = "u";
  private static final String OPTION_ECPOLICY = "e";

  public static final String NAME = "count";
  public static final String USAGE =
      "[-" + OPTION_QUOTA + "] [-" + OPTION_HUMAN + "] [-" + OPTION_HEADER
          + "] [-" + OPTION_TYPE + " [<storage type>]] [-" +
          OPTION_QUOTA_AND_USAGE + "] [-" + OPTION_EXCLUDE_SNAPSHOT
          + "] [-" + OPTION_ECPOLICY
          + "] <path> ...";
  public static final String DESCRIPTION =
      "Count the number of directories, files and bytes under the paths\n" +
          "that match the specified file pattern.  The output columns are:\n" +
          StringUtils.join(ContentSummary.getHeaderFields(), ' ') +
          " PATHNAME\n" +
          "or, with the -" + OPTION_QUOTA + " option:\n" +
          StringUtils.join(ContentSummary.getQuotaHeaderFields(), ' ') + "\n" +
          "      " +
          StringUtils.join(ContentSummary.getHeaderFields(), ' ') +
          " PATHNAME\n" +
          "The -" + OPTION_HUMAN +
          " option shows file sizes in human readable format.\n" +
          "The -" + OPTION_HEADER + " option displays a header line.\n" +
          "The -" + OPTION_EXCLUDE_SNAPSHOT + " option excludes snapshots " +
          "from being calculated. \n" +
          "The -" + OPTION_TYPE + " option displays quota by storage types.\n" +
          "It should be used with -" + OPTION_QUOTA + " or -" +
          OPTION_QUOTA_AND_USAGE + " option, otherwise it will be ignored.\n" +
          "If a comma-separated list of storage types is given after the -" +
          OPTION_TYPE + " option, \n" +
          "it displays the quota and usage for the specified types. \n" +
          "Otherwise, it displays the quota and usage for all the storage \n" +
          "types that support quota. The list of possible storage " +
          "types(case insensitive):\n" +
          "ram_disk, ssd, disk and archive.\n" +
          "It can also pass the value '', 'all' or 'ALL' to specify all " +
          "the storage types.\n" +
          "The -" + OPTION_QUOTA_AND_USAGE + " option shows the quota and \n" +
          "the usage against the quota without the detailed content summary."+
          "The -"+ OPTION_ECPOLICY +" option shows the erasure coding policy.";

  private boolean showQuotas;
  private boolean humanReadable;
  private boolean showQuotabyType;
  private List<StorageType> storageTypes = null;
  private boolean showQuotasAndUsageOnly;
  private boolean excludeSnapshots;
  private boolean displayECPolicy;

  /** Constructor */
  public Count() {}
  
  /** Constructor
   * @deprecated invoke via {@link FsShell}
   * @param cmd the count command
   * @param pos the starting index of the arguments
   * @param conf configuration
   */
  @Deprecated
  public Count(String[] cmd, int pos, Configuration conf) {
    super(conf);
    this.args = Arrays.copyOfRange(cmd, pos, cmd.length);
  }

  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE,
        OPTION_QUOTA, OPTION_HUMAN, OPTION_HEADER, OPTION_QUOTA_AND_USAGE,
        OPTION_EXCLUDE_SNAPSHOT,
        OPTION_ECPOLICY);
    cf.addOptionWithValue(OPTION_TYPE);
    cf.parse(args);
    if (args.isEmpty()) { // default path is the current working directory
      args.add(".");
    }
    showQuotas = cf.getOpt(OPTION_QUOTA);
    humanReadable = cf.getOpt(OPTION_HUMAN);
    showQuotasAndUsageOnly = cf.getOpt(OPTION_QUOTA_AND_USAGE);
    excludeSnapshots = cf.getOpt(OPTION_EXCLUDE_SNAPSHOT);
    displayECPolicy = cf.getOpt(OPTION_ECPOLICY);

    if (showQuotas || showQuotasAndUsageOnly) {
      String types = cf.getOptValue(OPTION_TYPE);

      if (null != types) {
        showQuotabyType = true;
        storageTypes = getAndCheckStorageTypes(types);
      } else {
        showQuotabyType = false;
      }
      if (excludeSnapshots) {
        out.println(OPTION_QUOTA + " or " + OPTION_QUOTA_AND_USAGE + " option "
            + "is given, the -x option is ignored.");
        excludeSnapshots = false;
      }
    }

    if (cf.getOpt(OPTION_HEADER)) {
      StringBuilder headString = new StringBuilder();
      if (showQuotabyType) {
        headString.append(QuotaUsage.getStorageTypeHeader(storageTypes));
      } else {
        if (showQuotasAndUsageOnly) {
          headString.append(QuotaUsage.getHeader());
        } else {
          headString.append(ContentSummary.getHeader(showQuotas));
        }
      }
      if(displayECPolicy){
        headString.append("ERASURECODING_POLICY ");
      }
      headString.append("PATHNAME");
      out.println(headString.toString());
    }
  }

  private List<StorageType> getAndCheckStorageTypes(String types) {
    if ("".equals(types) || "all".equalsIgnoreCase(types)) {
      return StorageType.getTypesSupportingQuota();
    }

    String[] typeArray = StringUtils.split(types, ',');
    List<StorageType> stTypes = new ArrayList<>();

    for (String t : typeArray) {
      stTypes.add(StorageType.parseStorageType(t));
    }

    return stTypes;
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    StringBuilder outputString = new StringBuilder();
    if (showQuotasAndUsageOnly || showQuotabyType) {
      QuotaUsage usage = src.fs.getQuotaUsage(src.path);
      outputString.append(usage.toString(
          isHumanReadable(), showQuotabyType, storageTypes));
    } else {
      ContentSummary summary = src.fs.getContentSummary(src.path);
      outputString.append(summary.toString(
          showQuotas, isHumanReadable(), excludeSnapshots));
    }
    if(displayECPolicy){
      ContentSummary summary = src.fs.getContentSummary(src.path);
      if(!summary.getErasureCodingPolicy().equals("Replicated")){
        outputString.append("EC:");
      }
      outputString.append(summary.getErasureCodingPolicy());
      outputString.append(" ");
    }
    outputString.append(src);
    out.println(outputString.toString());
  }

  /**
   * Should quotas get shown as part of the report?
   * @return if quotas should be shown then true otherwise false
   */
  @InterfaceAudience.Private
  boolean isShowQuotas() {
    return showQuotas;
  }
  
  /**
   * Should sizes be shown in human readable format rather than bytes?
   * @return true if human readable format
   */
  @InterfaceAudience.Private
  boolean isHumanReadable() {
    return humanReadable;
  }

  /**
   * should print quota by storage types
   * @return true if enables quota by storage types
   */
  @InterfaceAudience.Private
  boolean isShowQuotabyType() {
    return showQuotabyType;
  }

  /**
   * show specified storage types
   * @return specified storagetypes
   */
  @InterfaceAudience.Private
  List<StorageType> getStorageTypes() {
    return storageTypes;
  }

}
