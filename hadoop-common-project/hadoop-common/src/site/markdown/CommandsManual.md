<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

* [Hadoop Commands Guide](#Hadoop_Commands_Guide)
    * [Overview](#Overview)
        * [Generic Options](#Generic_Options)
* [Hadoop Common Commands](#Hadoop_Common_Commands)
    * [User Commands](#User_Commands)
        * [archive](#archive)
        * [checknative](#checknative)
        * [classpath](#classpath)
        * [credential](#credential)
        * [distcp](#distcp)
        * [fs](#fs)
        * [jar](#jar)
        * [key](#key)
        * [trace](#trace)
        * [version](#version)
        * [CLASSNAME](#CLASSNAME)
    * [Administration Commands](#Administration_Commands)
        * [daemonlog](#daemonlog)

Hadoop Commands Guide
=====================

Overview
--------

All hadoop commands are invoked by the `bin/hadoop` script. Running the
hadoop script without any arguments prints the description for all
commands.

Usage: `hadoop [--config confdir] [--loglevel loglevel] [COMMAND] [GENERIC_OPTIONS] [COMMAND_OPTIONS]`

| FIELD | Description |
|:---- |:---- |
| `--config confdir` | Overwrites the default Configuration directory.  Default is `${HADOOP_HOME}/conf`. |
| `--loglevel loglevel` | Overwrites the log level. Valid log levels are FATAL, ERROR, WARN, INFO, DEBUG, and TRACE. Default is INFO. |
| GENERIC\_OPTIONS | The common set of options supported by multiple commands. |
| COMMAND\_OPTIONS | Various commands with their options are described in this documention for the Hadoop common sub-project. HDFS and YARN are covered in other documents. |

### Generic Options

Many subcommands honor a common set of configuration options to alter their behavior:

| GENERIC\_OPTION | Description |
|:---- |:---- |
| `-archives <comma separated list of archives> ` | Specify comma separated archives to be unarchived on the compute machines. Applies only to job. |
| `-conf <configuration file> ` | Specify an application configuration file. |
| `-D <property>=<value> ` | Use value for given property. |
| `-files <comma separated list of files> ` | Specify comma separated files to be copied to the map reduce cluster. Applies only to job. |
| `-jt <local> or <resourcemanager:port>` | Specify a ResourceManager. Applies only to job. |
| `-libjars <comma seperated list of jars> ` | Specify comma separated jar files to include in the classpath. Applies only to job. |

Hadoop Common Commands
======================

All of these commands are executed from the `hadoop` shell command. They have been broken up into [User Commands](#User_Commands) and [Administration Commands](#Administration_Commands).

User Commands
-------------

Commands useful for users of a hadoop cluster.

### `archive`

Creates a hadoop archive. More information can be found at [Hadoop Archives Guide](../../hadoop-archives/HadoopArchives.html).

### `checknative`

Usage: `hadoop checknative [-a] [-h] `

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-a` | Check all libraries are available. |
| `-h` | print help |

This command checks the availability of the Hadoop native code. See [Native Libaries](./NativeLibraries.html) for more information. By default, this command only checks the availability of libhadoop.

### `classpath`

Usage: `hadoop classpath [--glob |--jar <path> |-h |--help]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `--glob` | expand wildcards |
| `--jar` *path* | write classpath as manifest in jar named *path* |
| `-h`, `--help` | print help |

Prints the class path needed to get the Hadoop jar and the required libraries. If called without arguments, then prints the classpath set up by the command scripts, which is likely to contain wildcards in the classpath entries. Additional options print the classpath after wildcard expansion or write the classpath into the manifest of a jar file. The latter is useful in environments where wildcards cannot be used and the expanded classpath exceeds the maximum supported command line length.

### `credential`

Usage: `hadoop credential <subcommand> [options]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| create *alias* [-provider *provider-path*] | Prompts the user for a credential to be stored as the given alias. The *hadoop.security.credential.provider.path* within the core-site.xml file will be used unless a `-provider` is indicated. |
| delete *alias* [-provider *provider-path*] [-f] | Deletes the credential with the provided alias. The *hadoop.security.credential.provider.path* within the core-site.xml file will be used unless a `-provider` is indicated. The command asks for confirmation unless `-f` is specified |
| list [-provider *provider-path*] | Lists all of the credential aliases The *hadoop.security.credential.provider.path* within the core-site.xml file will be used unless a `-provider` is indicated. |

Command to manage credentials, passwords and secrets within credential providers.

The CredentialProvider API in Hadoop allows for the separation of applications and how they store their required passwords/secrets. In order to indicate a particular provider type and location, the user must provide the *hadoop.security.credential.provider.path* configuration element in core-site.xml or use the command line option `-provider` on each of the following commands. This provider path is a comma-separated list of URLs that indicates the type and location of a list of providers that should be consulted. For example, the following path: `user:///,jceks://file/tmp/test.jceks,jceks://hdfs@nn1.example.com/my/path/test.jceks`

indicates that the current user's credentials file should be consulted through the User Provider, that the local file located at `/tmp/test.jceks` is a Java Keystore Provider and that the file located within HDFS at `nn1.example.com/my/path/test.jceks` is also a store for a Java Keystore Provider.

When utilizing the credential command it will often be for provisioning a password or secret to a particular credential store provider. In order to explicitly indicate which provider store to use the `-provider` option should be used. Otherwise, given a path of multiple providers, the first non-transient provider will be used. This may or may not be the one that you intended.

Example: `hadoop credential list -provider jceks://file/tmp/test.jceks`

### `distcp`

Copy file or directories recursively. More information can be found at [Hadoop DistCp Guide](../../hadoop-distcp/DistCp.html).

### `fs`

This command is documented in the [File System Shell Guide](./FileSystemShell.html). It is a synonym for `hdfs dfs` when HDFS is in use.

### `jar`

Usage: `hadoop jar <jar> [mainClass] args...`

Runs a jar file.

Use [`yarn jar`](../../hadoop-yarn/hadoop-yarn-site/YarnCommands.html#jar) to launch YARN applications instead.

### `key`

Usage: `hadoop key <subcommand> [options]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| create *keyname* [-cipher *cipher*] [-size *size*] [-description *description*] [-attr *attribute=value*] [-provider *provider*] [-help] | Creates a new key for the name specified by the *keyname* argument within the provider specified by the `-provider` argument. You may specify a cipher with the `-cipher` argument. The default cipher is currently "AES/CTR/NoPadding". The default keysize is 128. You may specify the requested key length using the `-size` argument. Arbitrary attribute=value style attributes may be specified using the `-attr` argument. `-attr` may be specified multiple times, once per attribute. |
| roll *keyname* [-provider *provider*] [-help] | Creates a new version for the specified key within the provider indicated using the `-provider` argument |
| delete *keyname* [-provider *provider*] [-f] [-help] | Deletes all versions of the key specified by the *keyname* argument from within the provider specified by `-provider`. The command asks for user confirmation unless `-f` is specified. |
| list [-provider *provider*] [-metadata] [-help] | Displays the keynames contained within a particular provider as configured in core-site.xml or specified with the `-provider` argument. `-metadata` displays the metadata. |
| -help | Prints usage of this command |

Manage keys via the KeyProvider. For details on KeyProviders, see the [Transparent Encryption Guide](../hadoop-hdfs/TransparentEncryption.html).

NOTE: Some KeyProviders (e.g. org.apache.hadoop.crypto.key.JavaKeyStoreProvider) does not support uppercase key names.

### `trace`

View and modify Hadoop tracing settings. See the [Tracing Guide](./Tracing.html).

### `version`

Usage: `hadoop version`

Prints the version.

### `CLASSNAME`

Usage: `hadoop CLASSNAME`

Runs the class named `CLASSNAME`.

Administration Commands
-----------------------

Commands useful for administrators of a hadoop cluster.

### `daemonlog`

Usage:

    hadoop daemonlog -getlevel <host:httpport> <classname>
    hadoop daemonlog -setlevel <host:httpport> <classname> <level>

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-getlevel` *host:httpport* *classname* | Prints the log level of the log identified by a qualified *classname*, in the daemon running at *host:httpport*. This command internally connects to `http://<host:httpport>/logLevel?log=<classname>` |
| `-setlevel` *host:httpport* *classname* *level* | Sets the log level of the log identified by a qualified *classname*, in the daemon running at *host:httpport*. This command internally connects to `http://<host:httpport>/logLevel?log=<classname>&level=<level>` |

Get/Set the log level for a Log identified by a qualified class name in the daemon.

	Example: $ bin/hadoop daemonlog -setlevel 127.0.0.1:50070 org.apache.hadoop.hdfs.server.namenode.NameNode DEBUG
