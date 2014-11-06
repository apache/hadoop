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
package org.apache.hadoop.registry.cli;

import static org.apache.hadoop.registry.client.binding.RegistryTypeUtils.*;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryCli extends Configured implements Tool {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryCli.class);
	protected final PrintStream sysout;
	protected final PrintStream syserr;


	private RegistryOperations registry;

  static final String LS_USAGE = "ls pathName";
  static final String RESOLVE_USAGE = "resolve pathName";
  static final String BIND_USAGE =
      "bind -inet  -api apiName -p portNumber -h hostName  pathName" + "\n"
      + "bind -webui uriString -api apiName  pathName" + "\n"
      + "bind -rest uriString -api apiName  pathName";
  static final String MKNODE_USAGE = "mknode directoryName";
  static final String RM_USAGE = "rm pathName";
  static final String USAGE =
      "\n" + LS_USAGE + "\n" + RESOLVE_USAGE + "\n" + BIND_USAGE + "\n" +
      MKNODE_USAGE + "\n" + RM_USAGE;



	public RegistryCli(PrintStream sysout, PrintStream syserr) {
	    super(new YarnConfiguration());
    this.sysout = sysout;
    this.syserr = syserr;
	}


  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void main(String[] args) throws Exception {
    RegistryCli cli = new RegistryCli(System.out, System.err);
    int res = ToolRunner.run(cli, args);
    System.exit(res);
  }

  private int usageError(String err, String usage) {
    syserr.println("Error: " + err);
    syserr.println("Usage: " + usage);
    return -1;
  }

  private boolean validatePath(String path) {
    if (!path.startsWith("/")) {
      syserr.println("Path must start with /; given path was: " + path);
      return false;
    }
    return true;
  }
  @Override
  public int run(String[] args) throws Exception {
    Preconditions.checkArgument(getConf() != null, "null configuration");
    registry = RegistryOperationsFactory.createInstance(
        new YarnConfiguration(getConf()));
    registry.start();
    if (args.length > 0) {
      if (args[0].equals("ls")) {
        return ls(args);
      } else if (args[0].equals("resolve")) {
        return resolve(args);
      } else if (args[0].equals("bind")) {
        return bind(args);
      } else if (args[0].equals("mknode")) {
        return mknode(args);
      } else if (args[0].equals("rm")) {
        return rm(args);
      }
    }
    return usageError("Invalid command: " + args[0], USAGE);
  }

  @SuppressWarnings("unchecked")
	public int ls(String [] args) {

		Options lsOption = new Options();
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine line = parser.parse(lsOption, args);

			List<String> argsList = line.getArgList();
			if (argsList.size() != 2) {
				return usageError("ls requires exactly one path argument", LS_USAGE);
		    }
			if (!validatePath(argsList.get(1)))
				return -1;

			try {
				List<String> children = registry.list(argsList.get(1));
        for (String child : children) {
          sysout.println(child);
        }
				return 0;

      } catch (Exception e) {
        syserr.println(analyzeException("ls", e, argsList));
      }
			return -1;
		} catch (ParseException exp) {
			return usageError("Invalid syntax " + exp, LS_USAGE);
		}
	}

  @SuppressWarnings("unchecked")
  public int resolve(String [] args) {
		Options resolveOption = new Options();
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine line = parser.parse(resolveOption, args);

			List<String> argsList = line.getArgList();
			if (argsList.size() != 2) {
				return usageError("resolve requires exactly one path argument", RESOLVE_USAGE);
		    }
			if (!validatePath(argsList.get(1)))
				return -1;

			try {
				ServiceRecord record = registry.resolve(argsList.get(1));

				for (Endpoint endpoint : record.external) {
						sysout.println(" Endpoint(ProtocolType="
								+ endpoint.protocolType + ", Api="
								+ endpoint.api + ");"
								+ " Addresses(AddressType="
								+ endpoint.addressType + ") are: ");

          for (Map<String, String> address : endpoint.addresses) {
            sysout.println("  [ ");
            for (Map.Entry<String, String> entry : address.entrySet()) {
              sysout.println("    " + entry.getKey()
                             + ": \"" + entry.getValue() + "\"");
            }
            sysout.println("  ]");
          }
          sysout.println();
        }
				return 0;
      } catch (Exception e) {
        syserr.println(analyzeException("resolve", e, argsList));
      }
			return -1;
		} catch (org.apache.commons.cli.ParseException exp) {
			return usageError("Invalid syntax " + exp, RESOLVE_USAGE);
		}

	}

	public int bind(String [] args) {
		Option rest = OptionBuilder.withArgName("rest")
				.hasArg()
				.withDescription("rest Option")
				.create("rest");
		Option webui = OptionBuilder.withArgName("webui")
				.hasArg()
				.withDescription("webui Option")
				.create("webui");
		Option inet = OptionBuilder.withArgName("inet")
				.withDescription("inet Option")
				.create("inet");
		Option port = OptionBuilder.withArgName("port")
				.hasArg()
				.withDescription("port to listen on [9999]")
				.create("p");
		Option host = OptionBuilder.withArgName("host")
				.hasArg()
				.withDescription("host name")
				.create("h");
		Option apiOpt = OptionBuilder.withArgName("api")
				.hasArg()
				.withDescription("api")
				.create("api");
		Options inetOption = new Options();
		inetOption.addOption(inet);
		inetOption.addOption(port);
		inetOption.addOption(host);
		inetOption.addOption(apiOpt);

		Options webuiOpt = new Options();
		webuiOpt.addOption(webui);
		webuiOpt.addOption(apiOpt);

		Options restOpt = new Options();
		restOpt.addOption(rest);
		restOpt.addOption(apiOpt);


    CommandLineParser parser = new GnuParser();
    ServiceRecord sr = new ServiceRecord();
    CommandLine line = null;
    if (args.length <= 1) {
      return usageError("Invalid syntax ", BIND_USAGE);
    }
    if (args[1].equals("-inet")) {
      int portNum;
      String hostName;
      String api;

      try {
        line = parser.parse(inetOption, args);
      } catch (ParseException exp) {
        return usageError("Invalid syntax " + exp.getMessage(), BIND_USAGE);
      }
      if (line.hasOption("inet") && line.hasOption("p") &&
          line.hasOption("h") && line.hasOption("api")) {
        portNum = Integer.parseInt(line.getOptionValue("p"));
        hostName = line.getOptionValue("h");
        api = line.getOptionValue("api");
        sr.addExternalEndpoint(
            inetAddrEndpoint(api, ProtocolTypes.PROTOCOL_HADOOP_IPC, hostName,
                portNum));

      } else {
        return usageError("Missing options: must have host, port and api",
            BIND_USAGE);
      }

    } else if (args[1].equals("-webui")) {
      try {
        line = parser.parse(webuiOpt, args);
      } catch (ParseException exp) {
        return usageError("Invalid syntax " + exp.getMessage(), BIND_USAGE);
      }
      if (line.hasOption("webui") && line.hasOption("api")) {
        URI theUri = null;
        try {
          theUri = new URI(line.getOptionValue("webui"));
        } catch (URISyntaxException e) {
          return usageError("Invalid URI: " + e.getMessage(), BIND_USAGE);
        }
        sr.addExternalEndpoint(webEndpoint(line.getOptionValue("api"), theUri));

      } else {
        return usageError("Missing options: must have value for uri and api",
            BIND_USAGE);
      }
    } else if (args[1].equals("-rest")) {
      try {
        line = parser.parse(restOpt, args);
      } catch (ParseException exp) {
        return usageError("Invalid syntax " + exp.getMessage(), BIND_USAGE);
      }
      if (line.hasOption("rest") && line.hasOption("api")) {
        URI theUri = null;
        try {
          theUri = new URI(line.getOptionValue("rest"));
        } catch (URISyntaxException e) {
          return usageError("Invalid URI: " + e.getMessage(), BIND_USAGE);
        }
        sr.addExternalEndpoint(
            restEndpoint(line.getOptionValue("api"), theUri));

      } else {
        return usageError("Missing options: must have value for uri and api",
            BIND_USAGE);
      }

    } else {
      return usageError("Invalid syntax", BIND_USAGE);
    }
    @SuppressWarnings("unchecked")
		List<String> argsList = line.getArgList();
		if (argsList.size() != 2) {
			return usageError("bind requires exactly one path argument", BIND_USAGE);
	    }
		if (!validatePath(argsList.get(1)))
			return -1;

		try {
			registry.bind(argsList.get(1), sr, BindFlags.OVERWRITE);
			return 0;
    } catch (Exception e) {
      syserr.println(analyzeException("bind", e, argsList));
    }

    return -1;
	}

  @SuppressWarnings("unchecked")
	public int mknode(String [] args) {
		Options mknodeOption = new Options();
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine line = parser.parse(mknodeOption, args);

			List<String> argsList = line.getArgList();
			if (argsList.size() != 2) {
				return usageError("mknode requires exactly one path argument", MKNODE_USAGE);
		    }
			if (!validatePath(argsList.get(1)))
				return -1;

			try {
				registry.mknode(args[1], false);
				return 0;
			} catch (Exception e) {
        syserr.println(analyzeException("mknode", e, argsList));
			}
			return -1;
		} catch (ParseException exp) {
			return usageError("Invalid syntax " + exp.toString(), MKNODE_USAGE);
		}
	}


  @SuppressWarnings("unchecked")
  public int rm(String[] args) {
		Option recursive = OptionBuilder.withArgName("recursive")
				.withDescription("delete recursively").create("r");

		Options rmOption = new Options();
		rmOption.addOption(recursive);

		boolean recursiveOpt = false;

		CommandLineParser parser = new GnuParser();
		try {
			CommandLine line = parser.parse(rmOption, args);

			List<String> argsList = line.getArgList();
			if (argsList.size() != 2) {
				return usageError("RM requires exactly one path argument", RM_USAGE);
		    }
			if (!validatePath(argsList.get(1)))
				return -1;

			try {
				if (line.hasOption("r")) {
					recursiveOpt = true;
				}

				registry.delete(argsList.get(1), recursiveOpt);
				return 0;
      } catch (Exception e) {
        syserr.println(analyzeException("rm", e, argsList));
      }
      return -1;
		} catch (ParseException exp) {
			return usageError("Invalid syntax " + exp.toString(), RM_USAGE);
		}
	}

  /**
   * Given an exception and a possibly empty argument list, generate
   * a diagnostics string for use in error messages
   * @param operation the operation that failed
   * @param e exception
   * @param argsList arguments list
   * @return a string intended for the user
   */
  String analyzeException(String operation,
      Exception e,
      List<String> argsList) {

    String pathArg = !argsList.isEmpty() ? argsList.get(1) : "(none)";
    if (LOG.isDebugEnabled()) {
      LOG.debug("Operation {} on path {} failed with exception {}",
          operation, pathArg, e, e);
    }
    if (e instanceof InvalidPathnameException) {
      return "InvalidPath :" + pathArg + ": " + e;
    }
    if (e instanceof PathNotFoundException) {
      return "Path not found: " + pathArg;
    }
    if (e instanceof NoRecordException) {
      return "No service record at path " + pathArg;
    }
    if (e instanceof AuthenticationFailedException) {
      return "Failed to authenticate to registry : " + e;
    }
    if (e instanceof NoPathPermissionsException) {
      return "No Permission to path: " + pathArg + ": " + e;
    }
    if (e instanceof AccessControlException) {
      return "No Permission to path: " + pathArg + ": " + e;
    }
    if (e instanceof InvalidRecordException) {
      return "Unable to read record at: " + pathArg + ": " + e;
    }
    if (e instanceof IOException) {
      return "IO Exception when accessing path :" + pathArg + ": " + e;
    }
    // something else went very wrong here
    return "Exception " + e;

  }
}
