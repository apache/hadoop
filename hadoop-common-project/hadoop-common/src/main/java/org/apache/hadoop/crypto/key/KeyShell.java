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

package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider.Metadata;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.tools.CommandShell;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program is the CLI utility for the KeyProvider facilities in Hadoop.
 */
public class KeyShell extends CommandShell {
  final static private String USAGE_PREFIX = "Usage: hadoop key " +
      "[generic options]\n";
  final static private String COMMANDS =
      "   [-help]\n" +
      "   [" + CreateCommand.USAGE + "]\n" +
      "   [" + RollCommand.USAGE + "]\n" +
      "   [" + DeleteCommand.USAGE + "]\n" +
      "   [" + ListCommand.USAGE + "]\n" +
      "   [" + InvalidateCacheCommand.USAGE + "]\n";
  private static final String LIST_METADATA = "keyShell.list.metadata";
  @VisibleForTesting
  public static final String NO_VALID_PROVIDERS =
      "There are no valid (non-transient) providers configured.\n" +
      "No action has been taken. Use the -provider option to specify\n" +
      "a provider. If you want to use a transient provider then you\n" +
      "MUST use the -provider argument.";

  private boolean interactive = true;

  /** If true, fail if the provider requires a password and none is given. */
  private boolean strict = false;

  private boolean userSuppliedProvider = false;

  /**
   * Parse the command line arguments and initialize the data.
   * <pre>
   * % hadoop key create keyName [-size size] [-cipher algorithm]
   *    [-provider providerPath]
   * % hadoop key roll keyName [-provider providerPath]
   * % hadoop key list [-provider providerPath]
   * % hadoop key delete keyName [-provider providerPath] [-i]
   * % hadoop key invalidateCache keyName [-provider providerPath]
   * </pre>
   * @param args Command line arguments.
   * @return 0 on success, 1 on failure.
   * @throws IOException
   */
  @Override
  protected int init(String[] args) throws IOException {
    final Options options = KeyProvider.options(getConf());
    final Map<String, String> attributes = new HashMap<String, String>();

    for (int i = 0; i < args.length; i++) { // parse command line
      boolean moreTokens = (i < args.length - 1);
      if (args[i].equals("create")) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }
        setSubCommand(new CreateCommand(keyName, options));
        if ("-help".equals(keyName)) {
          return 1;
        }
      } else if (args[i].equals("delete")) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }
        setSubCommand(new DeleteCommand(keyName));
        if ("-help".equals(keyName)) {
          return 1;
        }
      } else if (args[i].equals("roll")) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }
        setSubCommand(new RollCommand(keyName));
        if ("-help".equals(keyName)) {
          return 1;
        }
      } else if ("list".equals(args[i])) {
        setSubCommand(new ListCommand());
      } else if ("invalidateCache".equals(args[i])) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }
        setSubCommand(new InvalidateCacheCommand(keyName));
        if ("-help".equals(keyName)) {
          return 1;
        }
      } else if ("-size".equals(args[i]) && moreTokens) {
        options.setBitLength(Integer.parseInt(args[++i]));
      } else if ("-cipher".equals(args[i]) && moreTokens) {
        options.setCipher(args[++i]);
      } else if ("-description".equals(args[i]) && moreTokens) {
        options.setDescription(args[++i]);
      } else if ("-attr".equals(args[i]) && moreTokens) {
        final String attrval[] = args[++i].split("=", 2);
        final String attr = attrval[0].trim();
        final String val = attrval[1].trim();
        if (attr.isEmpty() || val.isEmpty()) {
          getOut().println("\nAttributes must be in attribute=value form, " +
              "or quoted\nlike \"attribute = value\"\n");
          return 1;
        }
        if (attributes.containsKey(attr)) {
          getOut().println("\nEach attribute must correspond to only one " +
              "value:\natttribute \"" + attr + "\" was repeated\n");
          return 1;
        }
        attributes.put(attr, val);
      } else if ("-provider".equals(args[i]) && moreTokens) {
        userSuppliedProvider = true;
        getConf().set(KeyProviderFactory.KEY_PROVIDER_PATH, args[++i]);
      } else if ("-metadata".equals(args[i])) {
        getConf().setBoolean(LIST_METADATA, true);
      } else if ("-f".equals(args[i]) || ("-force".equals(args[i]))) {
        interactive = false;
      } else if (args[i].equals("-strict")) {
        strict = true;
      } else if ("-help".equals(args[i])) {
        return 1;
      } else {
        ToolRunner.printGenericCommandUsage(getErr());
        return 1;
      }
    }

    if (!attributes.isEmpty()) {
      options.setAttributes(attributes);
    }

    return 0;
  }

  @Override
  public String getCommandUsage() {
    StringBuffer sbuf = new StringBuffer(USAGE_PREFIX + COMMANDS);
    String banner = StringUtils.repeat("=", 66);
    sbuf.append(banner + "\n");
    sbuf.append(CreateCommand.USAGE + ":\n\n" + CreateCommand.DESC + "\n");
    sbuf.append(banner + "\n");
    sbuf.append(RollCommand.USAGE + ":\n\n" + RollCommand.DESC + "\n");
    sbuf.append(banner + "\n");
    sbuf.append(DeleteCommand.USAGE + ":\n\n" + DeleteCommand.DESC + "\n");
    sbuf.append(banner + "\n");
    sbuf.append(ListCommand.USAGE + ":\n\n" + ListCommand.DESC + "\n");
    sbuf.append(banner + "\n");
    sbuf.append(InvalidateCacheCommand.USAGE + ":\n\n"
        + InvalidateCacheCommand.DESC + "\n");
    return sbuf.toString();
  }

  private abstract class Command extends SubCommand {
    protected KeyProvider provider = null;

    protected KeyProvider getKeyProvider() {
      KeyProvider prov = null;
      List<KeyProvider> providers;
      try {
        providers = KeyProviderFactory.getProviders(getConf());
        if (userSuppliedProvider) {
          prov = providers.get(0);
        } else {
          for (KeyProvider p : providers) {
            if (!p.isTransient()) {
              prov = p;
              break;
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace(getErr());
      }
      if (prov == null) {
        getOut().println(NO_VALID_PROVIDERS);
      }
      return prov;
    }

    protected void printProviderWritten() {
      getOut().println(provider + " has been updated.");
    }

    protected void warnIfTransientProvider() {
      if (provider.isTransient()) {
        getOut().println("WARNING: you are modifying a transient provider.");
      }
    }

    public abstract void execute() throws Exception;

    public abstract String getUsage();
  }

  private class ListCommand extends Command {
    public static final String USAGE =
        "list [-provider <provider>] [-strict] [-metadata] [-help]";
    public static final String DESC =
        "The list subcommand displays the keynames contained within\n" +
        "a particular provider as configured in core-site.xml or\n" +
        "specified with the -provider argument. -metadata displays\n" +
        "the metadata. If -strict is supplied, fail immediately if\n" +
        "the provider requires a password and none is given.";

    private boolean metadata = false;

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        rc = false;
      }
      metadata = getConf().getBoolean(LIST_METADATA, false);
      return rc;
    }

    public void execute() throws IOException {
      try {
        final List<String> keys = provider.getKeys();
        getOut().println("Listing keys for KeyProvider: " + provider);
        if (metadata) {
          final Metadata[] meta =
            provider.getKeysMetadata(keys.toArray(new String[keys.size()]));
          for (int i = 0; i < meta.length; ++i) {
            getOut().println(keys.get(i) + " : " + meta[i]);
          }
        } else {
          for (String keyName : keys) {
            getOut().println(keyName);
          }
        }
      } catch (IOException e) {
        getOut().println("Cannot list keys for KeyProvider: " + provider);
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class RollCommand extends Command {
    public static final String USAGE =
        "roll <keyname> [-provider <provider>] [-strict] [-help]";
    public static final String DESC =
        "The roll subcommand creates a new version for the specified key\n" +
        "within the provider indicated using the -provider argument.\n" +
        "If -strict is supplied, fail immediately if the provider requires\n" +
        "a password and none is given.";

    private String keyName = null;

    public RollCommand(String keyName) {
      this.keyName = keyName;
    }

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        rc = false;
      }
      if (keyName == null) {
        getOut().println("Please provide a <keyname>.\n" +
            "See the usage description by using -help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws NoSuchAlgorithmException, IOException {
      try {
        warnIfTransientProvider();
        getOut().println("Rolling key version from KeyProvider: "
            + provider + "\n  for key name: " + keyName);
        try {
          provider.rollNewVersion(keyName);
          provider.flush();
          getOut().println(keyName + " has been successfully rolled.");
          printProviderWritten();
        } catch (NoSuchAlgorithmException e) {
          getOut().println("Cannot roll key: " + keyName +
              " within KeyProvider: " + provider + ".");
          throw e;
        }
      } catch (IOException e1) {
        getOut().println("Cannot roll key: " + keyName + " within KeyProvider: "
            + provider + ".");
        throw e1;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class DeleteCommand extends Command {
    public static final String USAGE =
        "delete <keyname> [-provider <provider>] [-strict] [-f] [-help]";
    public static final String DESC =
        "The delete subcommand deletes all versions of the key\n" +
        "specified by the <keyname> argument from within the\n" +
        "provider specified by -provider. The command asks for\n" +
        "user confirmation unless -f is specified. If -strict is\n" +
        "supplied, fail immediately if the provider requires a\n" +
        "password and none is given.";

    private String keyName = null;
    private boolean cont = true;

    public DeleteCommand(String keyName) {
      this.keyName = keyName;
    }

    @Override
    public boolean validate() {
      provider = getKeyProvider();
      if (provider == null) {
        return false;
      }
      if (keyName == null) {
        getOut().println("There is no keyName specified. Please specify a " +
            "<keyname>. See the usage description with -help.");
        return false;
      }
      if (interactive) {
        try {
          cont = ToolRunner
              .confirmPrompt("You are about to DELETE all versions of "
                  + " key " + keyName + " from KeyProvider "
                  + provider + ". Continue? ");
          if (!cont) {
            getOut().println(keyName + " has not been deleted.");
          }
          return cont;
        } catch (IOException e) {
          getOut().println(keyName + " will not be deleted. "
              + prettifyException(e));
        }
      }
      return true;
    }

    public void execute() throws IOException {
      warnIfTransientProvider();
      getOut().println("Deleting key: " + keyName + " from KeyProvider: "
          + provider);
      if (cont) {
        try {
          provider.deleteKey(keyName);
          provider.flush();
          getOut().println(keyName + " has been successfully deleted.");
          printProviderWritten();
        } catch (IOException e) {
          getOut().println(keyName + " has not been deleted.");
          throw e;
        }
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class CreateCommand extends Command {
    public static final String USAGE =
        "create <keyname> [-cipher <cipher>] [-size <size>]\n" +
        "                     [-description <description>]\n" +
        "                     [-attr <attribute=value>]\n" +
        "                     [-provider <provider>] [-strict]\n" +
        "                     [-help]";
    public static final String DESC =
        "The create subcommand creates a new key for the name specified\n" +
        "by the <keyname> argument within the provider specified by the\n" +
        "-provider argument. You may specify a cipher with the -cipher\n" +
        "argument. The default cipher is currently \"AES/CTR/NoPadding\".\n" +
        "The default keysize is 128. You may specify the requested key\n" +
        "length using the -size argument. Arbitrary attribute=value\n" +
        "style attributes may be specified using the -attr argument.\n" +
        "-attr may be specified multiple times, once per attribute.\n";

    private final String keyName;
    private final Options options;

    public CreateCommand(String keyName, Options options) {
      this.keyName = keyName;
      this.options = options;
    }

    public boolean validate() {
      boolean rc = true;
      try {
        provider = getKeyProvider();
        if (provider == null) {
          rc = false;
        } else if (provider.needsPassword()) {
          if (strict) {
            getOut().println(provider.noPasswordError());
            rc = false;
          } else {
            getOut().println(provider.noPasswordWarning());
          }
        }
      } catch (IOException e) {
        e.printStackTrace(getErr());
      }
      if (keyName == null) {
        getOut().println("Please provide a <keyname>. " +
            " See the usage description with -help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws IOException, NoSuchAlgorithmException {
      warnIfTransientProvider();
      try {
        provider.createKey(keyName, options);
        provider.flush();
        getOut().println(keyName + " has been successfully created " +
            "with options " + options.toString() + ".");
        printProviderWritten();
      } catch (InvalidParameterException e) {
        getOut().println(keyName + " has not been created.");
        throw e;
      } catch (IOException e) {
        getOut().println(keyName + " has not been created.");
        throw e;
      } catch (NoSuchAlgorithmException e) {
        getOut().println(keyName + " has not been created.");
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class InvalidateCacheCommand extends Command {
    public static final String USAGE =
        "invalidateCache <keyname> [-provider <provider>] [-help]";
    public static final String DESC =
        "The invalidateCache subcommand invalidates the cached key versions\n"
            + "of the specified key, on the provider indicated using the"
            + " -provider argument.\n";

    private String keyName = null;

    InvalidateCacheCommand(String keyName) {
      this.keyName = keyName;
    }

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        getOut().println("Invalid provider.");
        rc = false;
      }
      if (keyName == null) {
        getOut().println("Please provide a <keyname>.\n" +
            "See the usage description by using -help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws NoSuchAlgorithmException, IOException {
      try {
        warnIfTransientProvider();
        getOut().println("Invalidating cache on KeyProvider: "
            + provider + "\n  for key name: " + keyName);
        provider.invalidateCache(keyName);
        getOut().println("Cached keyversions of " + keyName
            + " has been successfully invalidated.");
        printProviderWritten();
      } catch (IOException e) {
        getOut().println("Cannot invalidate cache for key: " + keyName +
            " within KeyProvider: " + provider + ".");
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  @Override
  protected void printException(Exception e){
    getErr().println("Executing command failed with " +
        "the following exception: " + prettifyException(e));
  }

  private String prettifyException(Exception e) {
    return e.getClass().getSimpleName() + ": " +
        e.getLocalizedMessage().split("\n")[0];
  }

  /**
   * main() entry point for the KeyShell.  While strictly speaking the
   * return is void, it will System.exit() with a return code: 0 is for
   * success and 1 for failure.
   *
   * @param args Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new KeyShell(), args);
    System.exit(res);
  }
}
