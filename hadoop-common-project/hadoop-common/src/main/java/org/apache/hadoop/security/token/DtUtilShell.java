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

package org.apache.hadoop.security.token;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.CommandShell;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  DtUtilShell is a set of command line token file management operations.
 */
public class DtUtilShell extends CommandShell {
  private static final Logger LOG = LoggerFactory.getLogger(DtUtilShell.class);

  private static final String FORMAT_SUBSTRING = "[-format (" +
      DtFileOperations.FORMAT_JAVA + "|" +
      DtFileOperations.FORMAT_PB + ")]";
  public static final String DT_USAGE = "hadoop dtutil " +
      "[-keytab <keytab_file> -principal <principal_name>] " +
      "subcommand (help|print|get|edit|append|cancel|remove|renew) " +
         FORMAT_SUBSTRING + " [-alias <alias>] filename...";

  // command line options
  private static final String HELP = "help";
  private static final String KEYTAB = "-keytab";
  private static final String PRINCIPAL = "-principal";
  private static final String PRINT = "print";
  private static final String GET = "get";
  private static final String EDIT = "edit";
  private static final String APPEND = "append";
  private static final String CANCEL = "cancel";
  private static final String REMOVE = "remove";
  private static final String RENEW = "renew";
  private static final String RENEWER = "-renewer";
  private static final String SERVICE = "-service";
  private static final String ALIAS = "-alias";
  private static final String FORMAT = "-format";

  // configuration state from args, conf
  private String keytab = null;
  private String principal = null;
  private Text alias = null;
  private Text service = null;
  private String renewer = null;
  private String format = DtFileOperations.FORMAT_PB;
  private ArrayList<File> tokenFiles = null;
  private File firstFile = null;

  /**
   * Parse arguments looking for Kerberos keytab/principal.
   * If both are found: remove both from the argument list and attempt login.
   * If only one of the two is found: remove it from argument list, log warning
   * and do not attempt login.
   * If neither is found: return original args array, doing nothing.
   * Return the pruned args array if either flag is present.
   */
  private String[] maybeDoLoginFromKeytabAndPrincipal(String[] args)
      throws IOException{
    ArrayList<String> savedArgs = new ArrayList<String>(args.length);
    for (int i = 0; i < args.length; i++) {
      String current = args[i];
      if (current.equals(PRINCIPAL)) {
        principal = args[++i];
      } else if (current.equals(KEYTAB)) {
        keytab = args[++i];
      } else {
        savedArgs.add(current);
      }
    }
    int newSize = savedArgs.size();
    if (newSize != args.length) {
      if (principal != null && keytab != null) {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      } else {
        LOG.warn("-principal and -keytab not both specified!  " +
                 "Kerberos login not attempted.");
      }
      return savedArgs.toArray(new String[newSize]);
    }
    return args;
  }

  /**
   * Parse the command line arguments and initialize subcommand.
   * Also will attempt to perform Kerberos login if both -principal and -keytab
   * flags are passed in args array.
   * @param args
   * @return 0 if the argument(s) were recognized, 1 otherwise
   * @throws Exception
   */
  @Override
  protected int init(String[] args) throws Exception {
    if (0 == args.length) {
      return 1;
    }
    tokenFiles = new ArrayList<File>();
    args = maybeDoLoginFromKeytabAndPrincipal(args);
    for (int i = 0; i < args.length; i++) {
      if (i == 0) {
        String command = args[0];
        if (command.equals(HELP)) {
          return 1;
        } else if (command.equals(PRINT)) {
          setSubCommand(new Print());
        } else if (command.equals(GET)) {
          setSubCommand(new Get(args[++i]));
        } else if (command.equals(EDIT)) {
          setSubCommand(new Edit());
        } else if (command.equals(APPEND)) {
          setSubCommand(new Append());
        } else if (command.equals(CANCEL)) {
          setSubCommand(new Remove(true));
        } else if (command.equals(REMOVE)) {
          setSubCommand(new Remove(false));
        } else if (command.equals(RENEW)) {
          setSubCommand(new Renew());
        }
      } else if (args[i].equals(ALIAS)) {
        alias = new Text(args[++i]);
      } else if (args[i].equals(SERVICE)) {
        service = new Text(args[++i]);
      } else if (args[i].equals(RENEWER)) {
        renewer = args[++i];
      } else if (args[i].equals(FORMAT)) {
        format = args[++i];
        if (!format.equals(DtFileOperations.FORMAT_JAVA) &&
            !format.equals(DtFileOperations.FORMAT_PB)) {
          LOG.error("-format must be '" + DtFileOperations.FORMAT_JAVA +
                    "' or '" + DtFileOperations.FORMAT_PB + "' not '" +
                    format + "'");
          return 1;
        }
      } else {
        for (; i < args.length; i++) {
          File f = new File(args[i]);
          if (f.exists()) {
            tokenFiles.add(f);
          }
          if (firstFile == null) {
            firstFile = f;
          }
        }
        if (tokenFiles.size() == 0 && firstFile == null) {
          LOG.error("Must provide a filename to all commands.");
          return 1;
        }
      }
    }
    return 0;
  }

  @Override
  public String getCommandUsage() {
    return String.format(
        "%n%s%n   %s%n   %s%n   %s%n   %s%n   %s%n   %s%n   %s%n%n",
        DT_USAGE, (new Print()).getUsage(), (new Get()).getUsage(),
        (new Edit()).getUsage(), (new Append()).getUsage(),
        (new Remove(true)).getUsage(), (new Remove(false)).getUsage(),
        (new Renew()).getUsage());
  }

  private class Print extends SubCommand {
    public static final String PRINT_USAGE =
        "dtutil print [-alias <alias>] filename...";

    @Override
    public void execute() throws Exception {
      for (File tokenFile : tokenFiles) {
        DtFileOperations.printTokenFile(tokenFile, alias, getConf(), getOut());
      }
    }

    @Override
    public String getUsage() {
      return PRINT_USAGE;
    }
  }

  private class Get extends SubCommand {
    public static final String GET_USAGE = "dtutil get URL " +
        "[-service <scheme>] " + FORMAT_SUBSTRING +
        "[-alias <alias>] [-renewer <renewer>] filename";
    private static final String PREFIX_HTTP = "http://";
    private static final String PREFIX_HTTPS = "https://";

    private String url = null;

    public Get() { }

    public Get(String arg) {
      url = arg;
    }

    public boolean isGenericUrl() {
      return url.startsWith(PREFIX_HTTP) || url.startsWith(PREFIX_HTTPS);
    }

    public boolean validate() {
      if (service != null && !isGenericUrl()) {
        LOG.error("Only provide -service with http/https URL.");
        return false;
      }
      if (service == null && isGenericUrl()) {
        LOG.error("Must provide -service with http/https URL.");
        return false;
      }
      if (url.indexOf("://") == -1) {
        LOG.error("URL does not contain a service specification: " + url);
        return false;
      }
      return true;
    }

    @Override
    public void execute() throws Exception {
      DtFileOperations.getTokenFile(
          firstFile, format, alias, service, url, renewer, getConf());
    }

    @Override
    public String getUsage() {
      return GET_USAGE;
    }
  }

  private class Edit extends SubCommand {
    public static final String EDIT_USAGE =
        "dtutil edit -service <service> -alias <alias> " +
        FORMAT_SUBSTRING + "filename...";

    @Override
    public boolean validate() {
      if (service == null) {
        LOG.error("must pass -service field with dtutil edit command");
        return false;
      }
      if (alias == null) {
        LOG.error("must pass -alias field with dtutil edit command");
        return false;
      }
      return true;
    }

    @Override
    public void execute() throws Exception {
      for (File tokenFile : tokenFiles) {
        DtFileOperations.aliasTokenFile(
            tokenFile, format, alias, service, getConf());
      }
    }

    @Override
    public String getUsage() {
      return EDIT_USAGE;
    }
  }

  private class Append extends SubCommand {
    public static final String APPEND_USAGE =
        "dtutil append " + FORMAT_SUBSTRING + "filename...";

    @Override
    public void execute() throws Exception {
      DtFileOperations.appendTokenFiles(tokenFiles, format, getConf());
    }

    @Override
    public String getUsage() {
      return APPEND_USAGE;
    }
  }

  private class Remove extends SubCommand {
    public static final String REMOVE_USAGE =
        "dtutil remove -alias <alias> " + FORMAT_SUBSTRING + " filename...";
    public static final String CANCEL_USAGE =
        "dtutil cancel -alias <alias> " + FORMAT_SUBSTRING + " filename...";
    private boolean cancel = false;

    public Remove(boolean arg) {
      cancel = arg;
    }

    @Override
    public boolean validate() {
      if (alias == null) {
        LOG.error("-alias flag is not optional for remove or cancel");
        return false;
      }
      return true;
    }

    @Override
    public void execute() throws Exception {
      for (File tokenFile : tokenFiles) {
        DtFileOperations.removeTokenFromFile(
            cancel, tokenFile, format, alias, getConf());
      }
    }

    @Override
    public String getUsage() {
      if (cancel) {
        return CANCEL_USAGE;
      }
      return REMOVE_USAGE;
    }
  }

  private class Renew extends SubCommand {
    public static final String RENEW_USAGE =
        "dtutil renew -alias <alias> filename...";

    @Override
    public boolean validate() {
      if (alias == null) {
        LOG.error("-alias flag is not optional for renew");
        return false;
      }
      return true;
    }

    @Override
    public void execute() throws Exception {
      for (File tokenFile : tokenFiles) {
        DtFileOperations.renewTokenFile(tokenFile, format, alias, getConf());
      }
    }

    @Override
    public String getUsage() {
      return RENEW_USAGE;
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new DtUtilShell(), args));
  }
}
