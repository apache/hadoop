#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script which checks Java API compatibility between two revisions of the
# Java client.
#
# Originally sourced from Apache Kudu, which was based on the
# compatibility checker from the Apache HBase project, but ported to
# Python for better readability.

import logging
import os
import re
import shutil
import subprocess
import sys
import urllib2
try:
  import argparse
except ImportError:
  sys.stderr.write("Please install argparse, e.g. via `pip install argparse`.")
  sys.exit(2)

# Various relative paths
REPO_DIR = os.getcwd()

def check_output(*popenargs, **kwargs):
  r"""Run command with arguments and return its output as a byte string.
  Backported from Python 2.7 as it's implemented as pure python on stdlib.
  >>> check_output(['/usr/bin/python', '--version'])
  Python 2.6.2
  """
  process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
  output, _ = process.communicate()
  retcode = process.poll()
  if retcode:
    cmd = kwargs.get("args")
    if cmd is None:
      cmd = popenargs[0]
    error = subprocess.CalledProcessError(retcode, cmd)
    error.output = output
    raise error
  return output

def get_repo_dir():
  """ Return the path to the top of the repo. """
  dirname, _ = os.path.split(os.path.abspath(__file__))
  return os.path.join(dirname, "../..")

def get_scratch_dir():
  """ Return the path to the scratch dir that we build within. """
  scratch_dir = os.path.join(get_repo_dir(), "target", "compat-check")
  if not os.path.exists(scratch_dir):
    os.makedirs(scratch_dir)
  return scratch_dir

def get_java_acc_dir():
  """ Return the path where we check out the Java API Compliance Checker. """
  return os.path.join(get_repo_dir(), "target", "java-acc")


def clean_scratch_dir(scratch_dir):
  """ Clean up and re-create the scratch directory. """
  if os.path.exists(scratch_dir):
    logging.info("Removing scratch dir %s...", scratch_dir)
    shutil.rmtree(scratch_dir)
  logging.info("Creating empty scratch dir %s...", scratch_dir)
  os.makedirs(scratch_dir)


def checkout_java_tree(rev, path):
  """ Check out the Java source tree for the given revision into
  the given path. """
  logging.info("Checking out %s in %s", rev, path)
  os.makedirs(path)
  # Extract java source
  subprocess.check_call(["bash", '-o', 'pipefail', "-c",
                         ("git archive --format=tar %s | " +
                          "tar -C \"%s\" -xf -") % (rev, path)],
                        cwd=get_repo_dir())

def get_git_hash(revname):
  """ Convert 'revname' to its SHA-1 hash. """
  return check_output(["git", "rev-parse", revname],
                      cwd=get_repo_dir()).strip()

def get_repo_name():
  """Get the name of the repo based on the git remote."""
  remotes = check_output(["git", "remote", "-v"],
                         cwd=get_repo_dir()).strip().split("\n")
  # Example output:
  # origin	https://github.com/apache/hadoop.git (fetch)
  # origin	https://github.com/apache/hadoop.git (push)
  remote_url = remotes[0].split("\t")[1].split(" ")[0]
  remote = remote_url.split("/")[-1]
  if remote.endswith(".git"):
    remote = remote[:-4]
  return remote

def build_tree(java_path):
  """ Run the Java build within 'path'. """
  logging.info("Building in %s...", java_path)
  subprocess.check_call(["mvn", "-DskipTests", "-Dmaven.javadoc.skip=true",
                         "package"],
                        cwd=java_path)


def checkout_java_acc(force):
  """
  Check out the Java API Compliance Checker. If 'force' is true, will
  re-download even if the directory exists.
  """
  acc_dir = get_java_acc_dir()
  if os.path.exists(acc_dir):
    logging.info("Java ACC is already downloaded.")
    if not force:
      return
    logging.info("Forcing re-download.")
    shutil.rmtree(acc_dir)

  logging.info("Downloading Java ACC...")

  url = "https://github.com/lvc/japi-compliance-checker/archive/1.8.tar.gz"
  scratch_dir = get_scratch_dir()
  path = os.path.join(scratch_dir, os.path.basename(url))
  jacc = urllib2.urlopen(url)
  with open(path, 'wb') as w:
    w.write(jacc.read())

  subprocess.check_call(["tar", "xzf", path],
                        cwd=scratch_dir)

  shutil.move(os.path.join(scratch_dir, "japi-compliance-checker-1.8"),
              os.path.join(acc_dir))


def find_jars(path):
  """ Return a list of jars within 'path' to be checked for compatibility. """
  all_jars = set(check_output(["find", path, "-name", "*.jar"]).splitlines())

  return [j for j in all_jars if (
      "-tests" not in j and
      "-sources" not in j and
      "-with-dependencies" not in j)]

def write_xml_file(path, version, jars):
  """Write the XML manifest file for JACC."""
  with open(path, "wt") as f:
    f.write("<version>" + version + "</version>\n")
    f.write("<archives>")
    for j in jars:
      f.write(j + "\n")
    f.write("</archives>")

def run_java_acc(src_name, src_jars, dst_name, dst_jars, annotations):
  """ Run the compliance checker to compare 'src' and 'dst'. """
  logging.info("Will check compatibility between original jars:\n\t%s\n" +
               "and new jars:\n\t%s",
               "\n\t".join(src_jars),
               "\n\t".join(dst_jars))

  java_acc_path = os.path.join(get_java_acc_dir(), "japi-compliance-checker.pl")

  src_xml_path = os.path.join(get_scratch_dir(), "src.xml")
  dst_xml_path = os.path.join(get_scratch_dir(), "dst.xml")
  write_xml_file(src_xml_path, src_name, src_jars)
  write_xml_file(dst_xml_path, dst_name, dst_jars)

  out_path = os.path.join(get_scratch_dir(), "report.html")

  args = ["perl", java_acc_path,
          "-l", get_repo_name(),
          "-d1", src_xml_path,
          "-d2", dst_xml_path,
          "-report-path", out_path]

  if annotations is not None:
    annotations_path = os.path.join(get_scratch_dir(), "annotations.txt")
    with file(annotations_path, "w") as f:
      for ann in annotations:
        print >>f, ann
    args += ["-annotations-list", annotations_path]

  subprocess.check_call(args)

def filter_jars(jars, include_filters, exclude_filters):
  """Filter the list of JARs based on include and exclude filters."""
  filtered = []
  # Apply include filters
  for j in jars:
    found = False
    basename = os.path.basename(j)
    for f in include_filters:
      if f.match(basename):
        found = True
        break
    if found:
      filtered += [j]
    else:
      logging.debug("Ignoring JAR %s", j)
  # Apply exclude filters
  exclude_filtered = []
  for j in filtered:
    basename = os.path.basename(j)
    found = False
    for f in exclude_filters:
      if f.match(basename):
        found = True
        break
    if found:
      logging.debug("Ignoring JAR %s", j)
    else:
      exclude_filtered += [j]

  return exclude_filtered


def main():
  """Main function."""
  logging.basicConfig(level=logging.INFO)
  parser = argparse.ArgumentParser(
      description="Run Java API Compliance Checker.")
  parser.add_argument("-f", "--force-download",
                      action="store_true",
                      help="Download dependencies (i.e. Java JAVA_ACC) " +
                      "even if they are already present")
  parser.add_argument("-i", "--include-file",
                      action="append",
                      dest="include_files",
                      help="Regex filter for JAR files to be included. " +
                      "Applied before the exclude filters. " +
                      "Can be specified multiple times.")
  parser.add_argument("-e", "--exclude-file",
                      action="append",
                      dest="exclude_files",
                      help="Regex filter for JAR files to be excluded. " +
                      "Applied after the include filters. " +
                      "Can be specified multiple times.")
  parser.add_argument("-a", "--annotation",
                      action="append",
                      dest="annotations",
                      help="Fully-qualified Java annotation. " +
                      "Java ACC will only check compatibility of " +
                      "annotated classes. Can be specified multiple times.")
  parser.add_argument("--skip-clean",
                      action="store_true",
                      help="Skip cleaning the scratch directory.")
  parser.add_argument("--skip-build",
                      action="store_true",
                      help="Skip building the projects.")
  parser.add_argument("src_rev", nargs=1, help="Source revision.")
  parser.add_argument("dst_rev", nargs="?", default="HEAD",
                      help="Destination revision. " +
                      "If not specified, will use HEAD.")

  if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(1)

  args = parser.parse_args()

  src_rev, dst_rev = args.src_rev[0], args.dst_rev

  logging.info("Source revision: %s", src_rev)
  logging.info("Destination revision: %s", dst_rev)

  # Construct the JAR regex patterns for filtering.
  include_filters = []
  if args.include_files is not None:
    for f in args.include_files:
      logging.info("Applying JAR filename include filter: %s", f)
      include_filters += [re.compile(f)]
  else:
    include_filters = [re.compile(".*")]

  exclude_filters = []
  if args.exclude_files is not None:
    for f in args.exclude_files:
      logging.info("Applying JAR filename exclude filter: %s", f)
      exclude_filters += [re.compile(f)]

  # Construct the annotation list
  annotations = args.annotations
  if annotations is not None:
    logging.info("Filtering classes using %d annotation(s):", len(annotations))
    for a in annotations:
      logging.info("\t%s", a)

  # Download deps.
  checkout_java_acc(args.force_download)

  # Set up the build.
  scratch_dir = get_scratch_dir()
  src_dir = os.path.join(scratch_dir, "src")
  dst_dir = os.path.join(scratch_dir, "dst")

  if args.skip_clean:
    logging.info("Skipping cleaning the scratch directory")
  else:
    clean_scratch_dir(scratch_dir)
    # Check out the src and dst source trees.
    checkout_java_tree(get_git_hash(src_rev), src_dir)
    checkout_java_tree(get_git_hash(dst_rev), dst_dir)

  # Run the build in each.
  if args.skip_build:
    logging.info("Skipping the build")
  else:
    build_tree(src_dir)
    build_tree(dst_dir)

  # Find the JARs.
  src_jars = find_jars(src_dir)
  dst_jars = find_jars(dst_dir)

  # Filter the JARs.
  src_jars = filter_jars(src_jars, include_filters, exclude_filters)
  dst_jars = filter_jars(dst_jars, include_filters, exclude_filters)

  if len(src_jars) == 0 or len(dst_jars) == 0:
    logging.error("No JARs found! Are your filters too strong?")
    sys.exit(1)

  run_java_acc(src_rev, src_jars,
               dst_rev, dst_jars, annotations)


if __name__ == "__main__":
  main()
