# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import with_statement

import ConfigParser
import hadoop.cloud.commands as commands
from hadoop.cloud.cluster import get_cluster
from hadoop.cloud.cluster import TimeoutException
from hadoop.cloud.providers.ec2 import Ec2Storage
from hadoop.cloud.util import merge_config_with_options
from hadoop.cloud.util import xstr
import logging
from optparse import OptionParser
from optparse import make_option
import os
import subprocess
import sys

version_file = os.path.join(sys.path[0], "VERSION")
VERSION = open(version_file, "r").read().strip()

DEFAULT_CLOUD_PROVIDER = 'ec2'

DEFAULT_CONFIG_DIR_NAME = '.hadoop-cloud'
DEFAULT_CONFIG_DIR = os.path.join(os.environ['HOME'], DEFAULT_CONFIG_DIR_NAME)
CONFIG_FILENAME = 'clusters.cfg'

CONFIG_DIR_OPTION = \
  make_option("--config-dir", metavar="CONFIG-DIR",
    help="The configuration directory.")

PROVIDER_OPTION = \
  make_option("--cloud-provider", metavar="PROVIDER",
    help="The cloud provider, e.g. 'ec2' for Amazon EC2.")

BASIC_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
]

LAUNCH_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("-a", "--ami", metavar="AMI",
    help="The AMI ID of the image to launch. (Amazon EC2 only. Deprecated, use \
--image-id.)"),
  make_option("-e", "--env", metavar="ENV", action="append",
    help="An environment variable to pass to instances. \
(May be specified multiple times.)"),
  make_option("-f", "--user-data-file", metavar="URL",
    help="The URL of the file containing user data to be made available to \
instances."),
  make_option("--image-id", metavar="ID",
    help="The ID of the image to launch."),
  make_option("-k", "--key-name", metavar="KEY-PAIR",
    help="The key pair to use when launching instances. (Amazon EC2 only.)"),
  make_option("-p", "--user-packages", metavar="PACKAGES",
    help="A space-separated list of packages to install on instances on start \
up."),
  make_option("-t", "--instance-type", metavar="TYPE",
    help="The type of instance to be launched. One of m1.small, m1.large, \
m1.xlarge, c1.medium, or c1.xlarge."),
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in."),
  make_option("--auto-shutdown", metavar="TIMEOUT_MINUTES",
    help="The time in minutes after launch when an instance will be \
automatically shut down."),
  make_option("--client-cidr", metavar="CIDR", action="append",
    help="The CIDR of the client, which is used to allow access through the \
firewall to the master node. (May be specified multiple times.)"),
  make_option("--public-key", metavar="FILE",
    help="The public key to authorize on launching instances. (Non-EC2 \
providers only.)"),
]

SNAPSHOT_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("-k", "--key-name", metavar="KEY-PAIR",
    help="The key pair to use when launching instances."),
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in."),
  make_option("--ssh-options", metavar="SSH-OPTIONS",
    help="SSH options to use."),
]

PLACEMENT_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in."),
]

FORCE_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("--force", action="store_true", default=False,
  help="Do not ask for confirmation."),
]

SSH_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("--ssh-options", metavar="SSH-OPTIONS",
    help="SSH options to use."),
]

def print_usage(script):
  print """Usage: %(script)s COMMAND [OPTIONS]
where COMMAND and [OPTIONS] may be one of:
  list [CLUSTER]                      list all running Hadoop clusters
                                        or instances in CLUSTER
  launch-master CLUSTER               launch or find a master in CLUSTER
  launch-slaves CLUSTER NUM_SLAVES    launch NUM_SLAVES slaves in CLUSTER
  launch-cluster CLUSTER NUM_SLAVES   launch a master and NUM_SLAVES slaves
                                        in CLUSTER
  create-formatted-snapshot CLUSTER   create an empty, formatted snapshot of
    SIZE                                size SIZE GiB
  list-storage CLUSTER                list storage volumes for CLUSTER
  create-storage CLUSTER ROLE         create volumes for NUM_INSTANCES instances
    NUM_INSTANCES SPEC_FILE             in ROLE for CLUSTER, using SPEC_FILE
  attach-storage ROLE                 attach storage volumes for ROLE to CLUSTER
  login CLUSTER                       log in to the master in CLUSTER over SSH
  proxy CLUSTER                       start a SOCKS proxy on localhost into the
                                        CLUSTER
  push CLUSTER FILE                   scp FILE to the master in CLUSTER
  exec CLUSTER CMD                    execute CMD on the master in CLUSTER
  terminate-cluster CLUSTER           terminate all instances in CLUSTER
  delete-cluster CLUSTER              delete the group information for CLUSTER
  delete-storage CLUSTER              delete all storage volumes for CLUSTER
  update-slaves-file CLUSTER          update the slaves file on the CLUSTER
                                        master

Use %(script)s COMMAND --help to see additional options for specific commands.
""" % locals()

def parse_options_and_config(command, option_list=[], extra_arguments=(),
                             unbounded_args=False):
  """
  Parse the arguments to command using the given option list, and combine with
  any configuration parameters.

  If unbounded_args is true then there must be at least as many extra arguments
  as specified by extra_arguments (the first argument is always CLUSTER).
  Otherwise there must be exactly the same number of arguments as
  extra_arguments.
  """
  expected_arguments = ["CLUSTER",]
  expected_arguments.extend(extra_arguments)
  (options_dict, args) = parse_options(command, option_list, expected_arguments,
                                       unbounded_args)

  config_dir = get_config_dir(options_dict)
  config_files = [os.path.join(config_dir, CONFIG_FILENAME)]
  if 'config_dir' not in options_dict:
    # if config_dir not set, then also search in current directory
    config_files.insert(0, CONFIG_FILENAME)

  config = ConfigParser.ConfigParser()
  read_files = config.read(config_files)
  logging.debug("Read %d configuration files: %s", len(read_files),
                ", ".join(read_files))
  cluster_name = args[0]
  opt = merge_config_with_options(cluster_name, config, options_dict)
  logging.debug("Options: %s", str(opt))
  return (opt, args, get_cluster(get_cloud_provider(opt))(cluster_name,
                                                          config_dir))

def parse_options(command, option_list=[], expected_arguments=(),
                  unbounded_args=False):
  """
  Parse the arguments to command using the given option list.

  If unbounded_args is true then there must be at least as many extra arguments
  as specified by extra_arguments (the first argument is always CLUSTER).
  Otherwise there must be exactly the same number of arguments as
  extra_arguments.
  """

  config_file_name = "%s/%s" % (DEFAULT_CONFIG_DIR_NAME, CONFIG_FILENAME)
  usage = """%%prog %s [options] %s

Options may also be specified in a configuration file called
%s located in the user's home directory.
Options specified on the command line take precedence over any in the
configuration file.""" % (command, " ".join(expected_arguments),
                          config_file_name)
  parser = OptionParser(usage=usage, version="%%prog %s" % VERSION,
                        option_list=option_list)
  parser.disable_interspersed_args()
  (options, args) = parser.parse_args(sys.argv[2:])
  if unbounded_args:
    if len(args) < len(expected_arguments):
      parser.error("incorrect number of arguments")
  elif len(args) != len(expected_arguments):
    parser.error("incorrect number of arguments")
  return (vars(options), args)

def get_config_dir(options_dict):
  config_dir = options_dict.get('config_dir')
  if not config_dir:
    config_dir = DEFAULT_CONFIG_DIR
  return config_dir

def get_cloud_provider(options_dict):
  provider = options_dict.get("cloud_provider", None)
  if provider is None:
    provider = DEFAULT_CLOUD_PROVIDER
  return provider

def check_options_set(options, option_names):
  for option_name in option_names:
    if options.get(option_name) is None:
      print "Option '%s' is missing. Aborting." % option_name
      sys.exit(1)

def check_launch_options_set(cluster, options):
  if cluster.get_provider_code() == 'ec2':
    if options.get('ami') is None and options.get('image_id') is None:
      print "One of ami or image_id must be specified. Aborting."
      sys.exit(1)
    check_options_set(options, ['key_name'])
  else:
    check_options_set(options, ['image_id', 'public_key'])

def get_image_id(cluster, options):
  if cluster.get_provider_code() == 'ec2':
    return options.get('image_id', options.get('ami'))
  else:
    return options.get('image_id')

def _prompt(prompt):
  """ Returns true if user responds "yes" to prompt. """
  return raw_input("%s [yes or no]: " % prompt).lower() == "yes"

def main():
  # Use HADOOP_CLOUD_LOGGING_LEVEL=DEBUG to enable debugging output.
  logging.basicConfig(level=getattr(logging,
                                    os.getenv("HADOOP_CLOUD_LOGGING_LEVEL",
                                              "INFO")))

  if len(sys.argv) < 2:
    print_usage(sys.argv[0])
    sys.exit(1)

  command = sys.argv[1]

  if command == 'list':
    (opt, args) = parse_options(command, BASIC_OPTIONS, unbounded_args=True)
    if len(args) == 0:
      commands.list_all(get_cloud_provider(opt))
    else:
      (opt, args, cluster) = parse_options_and_config(command, BASIC_OPTIONS)
      commands.list_cluster(cluster)

  elif command == 'launch-master':
    (opt, args, cluster) = parse_options_and_config(command, LAUNCH_OPTIONS)
    check_launch_options_set(cluster, opt)
    config_dir = get_config_dir(opt)
    commands.launch_master(cluster, config_dir, get_image_id(cluster, opt),
      opt.get('instance_type'),
      opt.get('key_name'), opt.get('public_key'), opt.get('user_data_file'),
      opt.get('availability_zone'), opt.get('user_packages'),
      opt.get('auto_shutdown'), opt.get('env'), opt.get('client_cidr'))
    commands.attach_storage(cluster, (commands.MASTER,))
    try:
      commands.wait_for_hadoop(cluster, 0)
    except TimeoutException:
      print "Timeout while waiting for Hadoop to start. Please check logs on" +\
        " master."
    commands.print_master_url(cluster)

  elif command == 'launch-slaves':
    (opt, args, cluster) = parse_options_and_config(command, LAUNCH_OPTIONS,
                                                    ("NUM_SLAVES",))
    number_of_slaves = int(args[1])
    check_launch_options_set(cluster, opt)
    config_dir = get_config_dir(opt)
    commands.launch_slaves(cluster, number_of_slaves, get_image_id(cluster, opt),
      opt.get('instance_type'),
      opt.get('key_name'), opt.get('public_key'), opt.get('user_data_file'),
      opt.get('availability_zone'), opt.get('user_packages'),
      opt.get('auto_shutdown'), opt.get('env'))
    commands.attach_storage(cluster, (commands.SLAVE,))
    commands.print_master_url(cluster)

  elif command == 'launch-cluster':
    (opt, args, cluster) = parse_options_and_config(command, LAUNCH_OPTIONS,
                                                    ("NUM_SLAVES",))
    number_of_slaves = int(args[1])
    check_launch_options_set(cluster, opt)
    config_dir = get_config_dir(opt)
    commands.launch_master(cluster, config_dir, get_image_id(cluster, opt),
      opt.get('instance_type'),
      opt.get('key_name'), opt.get('public_key'), opt.get('user_data_file'),
      opt.get('availability_zone'), opt.get('user_packages'),
      opt.get('auto_shutdown'), opt.get('env'), opt.get('client_cidr'))
    commands.launch_slaves(cluster, number_of_slaves, get_image_id(cluster, opt),
      opt.get('instance_type'),
      opt.get('key_name'), opt.get('public_key'), opt.get('user_data_file'),
      opt.get('availability_zone'), opt.get('user_packages'),
      opt.get('auto_shutdown'), opt.get('env'))
    commands.attach_storage(cluster, commands.ROLES)
    try:
      commands.wait_for_hadoop(cluster, number_of_slaves)
    except TimeoutException:
      print "Timeout while waiting for Hadoop to start. Please check logs on" +\
        " cluster."
    commands.print_master_url(cluster)

  elif command == 'login':
    (opt, args, cluster) = parse_options_and_config(command, SSH_OPTIONS)
    instances = cluster.check_running(commands.MASTER, 1)
    if not instances:
      sys.exit(1)
    subprocess.call('ssh %s root@%s' % \
                    (xstr(opt.get('ssh_options')), instances[0].public_ip),
                    shell=True)

  elif command == 'proxy':
    (opt, args, cluster) = parse_options_and_config(command, SSH_OPTIONS)
    instances = cluster.check_running(commands.MASTER, 1)
    if not instances:
      sys.exit(1)
    options = '-o "ConnectTimeout 10" -o "ServerAliveInterval 60" ' \
              '-N -D 6666'
    process = subprocess.Popen('ssh %s %s root@%s' %
      (xstr(opt.get('ssh_options')), options, instances[0].public_ip),
      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
      shell=True)
    print """export HADOOP_CLOUD_PROXY_PID=%s;
echo Proxy pid %s;""" % (process.pid, process.pid)

  elif command == 'push':
    (opt, args, cluster) = parse_options_and_config(command, SSH_OPTIONS,
                                                    ("FILE",))
    instances = cluster.check_running(commands.MASTER, 1)
    if not instances:
      sys.exit(1)
    subprocess.call('scp %s -r %s root@%s:' % (xstr(opt.get('ssh_options')),
                                               args[1], instances[0].public_ip),
                                               shell=True)

  elif command == 'exec':
    (opt, args, cluster) = parse_options_and_config(command, SSH_OPTIONS,
                                                    ("CMD",), True)
    instances = cluster.check_running(commands.MASTER, 1)
    if not instances:
      sys.exit(1)
    subprocess.call("ssh %s root@%s '%s'" % (xstr(opt.get('ssh_options')),
                                             instances[0].public_ip,
                                             " ".join(args[1:])), shell=True)

  elif command == 'terminate-cluster':
    (opt, args, cluster) = parse_options_and_config(command, FORCE_OPTIONS)
    cluster.print_status(commands.ROLES)
    if not opt["force"] and not _prompt("Terminate all instances?"):
      print "Not terminating cluster."
    else:
      print "Terminating cluster"
      cluster.terminate()

  elif command == 'delete-cluster':
    (opt, args, cluster) = parse_options_and_config(command, BASIC_OPTIONS)
    cluster.delete()

  elif command == 'create-formatted-snapshot':
    (opt, args, cluster) = parse_options_and_config(command, SNAPSHOT_OPTIONS,
                                                    ("SIZE",))
    size = int(args[1])
    check_options_set(opt, ['availability_zone', 'key_name'])
    ami_ubuntu_intrepid_x86 = 'ami-ec48af85' # use a general AMI
    Ec2Storage.create_formatted_snapshot(cluster, size,
                                         opt.get('availability_zone'),
                                         ami_ubuntu_intrepid_x86,
                                         opt.get('key_name'),
                                         xstr(opt.get('ssh_options')))

  elif command == 'list-storage':
    (opt, args, cluster) = parse_options_and_config(command, BASIC_OPTIONS)
    storage = cluster.get_storage()
    storage.print_status(commands.ROLES)

  elif command == 'create-storage':
    (opt, args, cluster) = parse_options_and_config(command, PLACEMENT_OPTIONS,
                                                    ("ROLE", "NUM_INSTANCES",
                                                     "SPEC_FILE"))
    storage = cluster.get_storage()
    role = args[1]
    number_of_instances = int(args[2])
    spec_file = args[3]
    check_options_set(opt, ['availability_zone'])
    storage.create(role, number_of_instances, opt.get('availability_zone'),
                   spec_file)
    storage.print_status(commands.ROLES)

  elif command == 'attach-storage':
    (opt, args, cluster) = parse_options_and_config(command, BASIC_OPTIONS,
                                                    ("ROLE",))
    storage = cluster.get_storage()
    role = args[1]
    storage.attach(role, cluster.get_instances_in_role(role, 'running'))
    storage.print_status(commands.ROLES)

  elif command == 'delete-storage':
    (opt, args, cluster) = parse_options_and_config(command, FORCE_OPTIONS)
    storage = cluster.get_storage()
    storage.print_status(commands.ROLES)
    if not opt["force"] and not _prompt("Delete all storage volumes? THIS WILL \
      PERMANENTLY DELETE ALL DATA"):
      print "Not deleting storage volumes."
    else:
      print "Deleting storage"
      for role in commands.ROLES:
        storage.delete(role)

  elif command == 'update-slaves-file':
    (opt, args, cluster) = parse_options_and_config(command, SSH_OPTIONS)
    check_options_set(opt, ['private_key'])
    ssh_options = xstr(opt.get('ssh_options'))
    instances = cluster.check_running(commands.MASTER, 1)
    if not instances:
      sys.exit(1)
    master = instances[0]
    slaves = cluster.get_instances_in_role(commands.SLAVE)
    with open('slaves', 'w') as f:
      for slave in slaves:
        f.write(slave.public_ip + "\n")
    subprocess.call('scp %s -r %s root@%s:/etc/hadoop/conf' % \
                    (ssh_options, 'slaves', master.public_ip), shell=True)

    # Copy private key
    private_key = opt.get('private_key')
    subprocess.call('scp %s -r %s root@%s:/root/.ssh/id_rsa' % \
                    (ssh_options, private_key, master.public_ip), shell=True)
    for slave in slaves:
      subprocess.call('scp %s -r %s root@%s:/root/.ssh/id_rsa' % \
                      (ssh_options, private_key, slave.public_ip), shell=True)

  else:
    print "Unrecognized command '%s'" % command
    print_usage(sys.argv[0])
    sys.exit(1)
