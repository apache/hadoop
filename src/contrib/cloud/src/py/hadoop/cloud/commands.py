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

"""High-level commands that a user may want to run"""

from __future__ import with_statement

from hadoop.cloud.cluster import get_cluster
from hadoop.cloud.cluster import InstanceUserData
from hadoop.cloud.cluster import TimeoutException
from hadoop.cloud.util import build_env_string
from hadoop.cloud.util import url_get
import logging
import os
import re
import socket
import sys
import time

logger = logging.getLogger(__name__)

MASTER = "master"
SLAVE = "slave"
# ROLES contains the set of all roles in a cluster. It may be expanded in the
# future to support HBase, or split namnode and jobtracker instances.
ROLES = (MASTER, SLAVE)

def _get_default_user_data_file_template(cluster):
  return os.path.join(sys.path[0], 'hadoop-%s-init-remote.sh' %
               cluster.get_provider_code())

def list_all(provider):
  """
  Find and print clusters that have a running 'master' instance
  """
  clusters = get_cluster(provider).get_clusters_with_role(MASTER)
  if not clusters:
    print "No running clusters"
  else:
    for cluster in clusters:
      print cluster

def list_cluster(cluster):
  cluster.print_status(ROLES)

def launch_master(cluster, config_dir, image_id, size_id, key_name, public_key,
                  user_data_file_template=None, placement=None,
                  user_packages=None, auto_shutdown=None, env_strings=[],
                  client_cidrs=[]):
  if user_data_file_template == None:
    user_data_file_template = _get_default_user_data_file_template(cluster)
  if cluster.check_running(MASTER, 0) == False:
    return  # don't proceed if another master is running
  ebs_mappings = ''
  storage = cluster.get_storage()
  if storage.has_any_storage((MASTER,)):
    ebs_mappings = storage.get_mappings_string_for_role(MASTER)
  replacements = { "%ENV%": build_env_string(env_strings, {
    "USER_PACKAGES": user_packages,
    "AUTO_SHUTDOWN": auto_shutdown,
    "EBS_MAPPINGS": ebs_mappings
  }) }
  instance_user_data = InstanceUserData(user_data_file_template, replacements)
  instance_ids = cluster.launch_instances(MASTER, 1, image_id, size_id,
                                          instance_user_data,
                                          key_name=key_name,
                                          public_key=public_key,
                                          placement=placement)
  print "Waiting for master to start (%s)" % str(instance_ids[0])
  try:
    cluster.wait_for_instances(instance_ids)
    print "Master started"
  except TimeoutException:
    print "Timeout while waiting for master instance to start."
    return
  cluster.print_status((MASTER,))
  master = cluster.check_running(MASTER, 1)[0]
  _authorize_client_ports(cluster, master, client_cidrs)
  _create_client_hadoop_site_file(cluster, config_dir, master)

def _authorize_client_ports(cluster, master, client_cidrs):
  if not client_cidrs:
    logger.debug("No client CIDRs specified, using local address.")
    client_ip = url_get('http://checkip.amazonaws.com/').strip()
    client_cidrs = ("%s/32" % client_ip,)
  logger.debug("Client CIDRs: %s", client_cidrs)
  for client_cidr in client_cidrs:
    # Allow access to port 80 on master from client
    cluster.authorize_role(MASTER, 80, 80, client_cidr)
    # Allow access to jobtracker UI on master from client
    # (so we can see when the cluster is ready)
    cluster.authorize_role(MASTER, 50030, 50030, client_cidr)
    # Allow access to namenode and jobtracker via public address from master
    # node
  master_ip = socket.gethostbyname(master.public_ip)
  cluster.authorize_role(MASTER, 8020, 8021, "%s/32" % master_ip)

def _create_client_hadoop_site_file(cluster, config_dir, master):
  cluster_dir = os.path.join(config_dir, cluster.name)
  aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
  aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
  if not os.path.exists(cluster_dir):
    os.makedirs(cluster_dir)
  with open(os.path.join(cluster_dir, 'hadoop-site.xml'), 'w') as f:
    f.write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
<property>
  <name>hadoop.job.ugi</name>
  <value>root,root</value>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://%(master)s:8020/</value>
</property>
<property>
  <name>mapred.job.tracker</name>
  <value>%(master)s:8021</value>
</property>
<property>
  <name>hadoop.socks.server</name>
  <value>localhost:6666</value>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.default</name>
  <value>org.apache.hadoop.net.SocksSocketFactory</value>
</property>
<property>
  <name>fs.s3.awsAccessKeyId</name>
  <value>%(aws_access_key_id)s</value>
</property>
<property>
  <name>fs.s3.awsSecretAccessKey</name>
  <value>%(aws_secret_access_key)s</value>
</property>
<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>%(aws_access_key_id)s</value>
</property>
<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>%(aws_secret_access_key)s</value>
</property>
</configuration>
""" % {'master': master.public_ip,
  'aws_access_key_id': aws_access_key_id,
  'aws_secret_access_key': aws_secret_access_key})

def launch_slaves(cluster, number, image_id, size_id, key_name,
                  public_key,
                  user_data_file_template=None, placement=None,
                  user_packages=None, auto_shutdown=None, env_strings=[]):
  if user_data_file_template == None:
    user_data_file_template = _get_default_user_data_file_template(cluster)
  instances = cluster.check_running(MASTER, 1)
  if not instances:
    return
  master = instances[0]
  ebs_mappings = ''
  storage = cluster.get_storage()
  if storage.has_any_storage((SLAVE,)):
    ebs_mappings = storage.get_mappings_string_for_role(SLAVE)
  replacements = { "%ENV%": build_env_string(env_strings, {
    "USER_PACKAGES": user_packages,
    "AUTO_SHUTDOWN": auto_shutdown,
    "EBS_MAPPINGS": ebs_mappings,
    "MASTER_HOST": master.public_ip
  }) }
  instance_user_data = InstanceUserData(user_data_file_template, replacements)
  instance_ids = cluster.launch_instances(SLAVE, number, image_id, size_id,
                                          instance_user_data,
                                          key_name=key_name,
                                          public_key=public_key,
                                          placement=placement)
  print "Waiting for slaves to start"
  try:
    cluster.wait_for_instances(instance_ids)
    print "Slaves started"
  except TimeoutException:
    print "Timeout while waiting for slave instances to start."
    return
  print
  cluster.print_status((SLAVE,))

def wait_for_hadoop(cluster, number, timeout=600):
  start_time = time.time()
  instances = cluster.check_running(MASTER, 1)
  if not instances:
    return
  master = instances[0]
  print "Waiting for jobtracker to start"
  previous_running = 0
  while True:
    if (time.time() - start_time >= timeout):
      raise TimeoutException()
    try:
      actual_running = _number_of_tasktrackers(master.public_ip, 1)
      break
    except IOError:
      pass
    sys.stdout.write(".")
    sys.stdout.flush()
    time.sleep(1)
  print
  if number > 0:
    print "Waiting for %d tasktrackers to start" % number
    while actual_running < number:
      if (time.time() - start_time >= timeout):
        raise TimeoutException()
      try:
        actual_running = _number_of_tasktrackers(master.public_ip, 5, 2)
        if actual_running != previous_running:
          sys.stdout.write("%d" % actual_running)
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(1)
        previous_running = actual_running
      except IOError:
        raise TimeoutException()
    print

# The optional ?type=active is a difference between Hadoop 0.18 and 0.20
NUMBER_OF_TASK_TRACKERS = re.compile(
  r'<a href="machines.jsp(?:\?type=active)?">(\d+)</a>')

def _number_of_tasktrackers(jt_hostname, timeout, retries=0):
  jt_page = url_get("http://%s:50030/jobtracker.jsp" % jt_hostname, timeout,
                    retries)
  m = NUMBER_OF_TASK_TRACKERS.search(jt_page)
  if m:
    return int(m.group(1))
  return 0

def print_master_url(cluster):
  instances = cluster.check_running(MASTER, 1)
  if not instances:
    return
  master = instances[0]
  print "Browse the cluster at http://%s/" % master.public_ip

def attach_storage(cluster, roles):
  storage = cluster.get_storage()
  if storage.has_any_storage(roles):
    print "Waiting 10 seconds before attaching storage"
    time.sleep(10)
    for role in roles:
      storage.attach(role, cluster.get_instances_in_role(role, 'running'))
    storage.print_status(roles)
