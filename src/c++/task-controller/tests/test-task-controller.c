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
#include "../task-controller.h"

#define HADOOP_CONF_DIR "/tmp"

int write_config_file(char *file_name) {
  FILE *file;
  char const *str =
      "mapred.local.dir=/tmp/testing1,/tmp/testing2,/tmp/testing3,/tmp/testing4\n";

  file = fopen(file_name, "w");
  if (file == NULL) {
    printf("Failed to open %s.\n", file_name);
    return EXIT_FAILURE;
  }
  fwrite(str, 1, strlen(str), file);
  fclose(file);
  return 0;
}

void test_check_variable_against_config() {

  // A temporary configuration directory
  char *conf_dir_templ = "/tmp/test-task-controller-conf-dir-XXXXXX";

  // To accomodate "/conf/taskcontroller.cfg"
  char template[strlen(conf_dir_templ) + strlen("/conf/taskcontroller.cfg")];

  strcpy(template, conf_dir_templ);
  char *temp_dir = mkdtemp(template);
  if (temp_dir == NULL) {
    printf("Couldn't create a temporary dir for conf.\n");
    goto cleanup;
  }

  // Set the configuration directory
  hadoop_conf_dir = strdup(temp_dir);

  // create the configuration directory
  strcat(template, "/conf");
  char *conf_dir = strdup(template);
  mkdir(conf_dir, S_IRWXU);

  // create the configuration file
  strcat(template, "/taskcontroller.cfg");
  if (write_config_file(template) != 0) {
    printf("Couldn't write the configuration file.\n");
    goto cleanup;
  }

  // Test obtaining a value for a key from the config
  char *config_values[4] = { "/tmp/testing1", "/tmp/testing2",
      "/tmp/testing3", "/tmp/testing4" };
  char *value = (char *) get_value("mapred.local.dir");
  if (strcmp(value, "/tmp/testing1,/tmp/testing2,/tmp/testing3,/tmp/testing4")
      != 0) {
    printf("Obtaining a value for a key from the config failed.\n");
    goto cleanup;
  }

  // Test the parsing of a multiple valued key from the config
  char **values = (char **) get_values("mapred.local.dir");
  char **values_ptr = values;
  int i = 0;
  while (*values_ptr != NULL) {
    printf(" value : %s\n", *values_ptr);
    if (strcmp(*values_ptr, config_values[i++]) != 0) {
      printf("Configured values are not read out properly. Test failed!");
      goto cleanup;;
    }
    values_ptr++;
  }

  if (check_variable_against_config("mapred.local.dir", "/tmp/testing5") == 0) {
    printf("Configuration should not contain /tmp/testing5! \n");
    goto cleanup;
  }

  if (check_variable_against_config("mapred.local.dir", "/tmp/testing4") != 0) {
    printf("Configuration should contain /tmp/testing4! \n");
    goto cleanup;
  }

  cleanup: if (value != NULL) {
    free(value);
  }
  if (values != NULL) {
    free(values);
  }
  if (hadoop_conf_dir != NULL) {
    free(hadoop_conf_dir);
  }
  unlink(template);
  rmdir(conf_dir);
  rmdir(hadoop_conf_dir);
}

void test_get_user_directory() {
  char *user_dir = (char *) get_user_directory("/tmp", "user");
  printf("user_dir obtained is %s\n", user_dir);
  int ret = 0;
  if (strcmp(user_dir, "/tmp/taskTracker/user") != 0) {
    ret = -1;
  }
  free(user_dir);
  assert(ret == 0);
}

void test_get_job_directory() {
  char *job_dir = (char *) get_job_directory("/tmp", "user",
      "job_200906101234_0001");
  printf("job_dir obtained is %s\n", job_dir);
  int ret = 0;
  if (strcmp(job_dir, "/tmp/taskTracker/user/jobcache/job_200906101234_0001")
      != 0) {
    ret = -1;
  }
  free(job_dir);
  assert(ret == 0);
}

void test_get_attempt_directory() {
  char *job_dir = (char *) get_job_directory("/tmp", "user",
      "job_200906101234_0001");
  printf("job_dir obtained is %s\n", job_dir);
  char *attempt_dir = (char *) get_attempt_directory(job_dir,
      "attempt_200906101234_0001_m_000000_0");
  printf("attempt_dir obtained is %s\n", attempt_dir);
  int ret = 0;
  if (strcmp(
      attempt_dir,
      "/tmp/taskTracker/user/jobcache/job_200906101234_0001/attempt_200906101234_0001_m_000000_0")
      != 0) {
    ret = -1;
  }
  free(job_dir);
  free(attempt_dir);
  assert(ret == 0);
}

void test_get_task_launcher_file() {
  char *job_dir = (char *) get_job_directory("/tmp", "user",
      "job_200906101234_0001");
  char *task_file = (char *) get_task_launcher_file(job_dir,
      "attempt_200906112028_0001_m_000000_0");
  printf("task_file obtained is %s\n", task_file);
  int ret = 0;
  if (strcmp(
      task_file,
      "/tmp/taskTracker/user/jobcache/job_200906101234_0001/attempt_200906112028_0001_m_000000_0/taskjvm.sh")
      != 0) {
    ret = -1;
  }
  free(task_file);
  assert(ret == 0);
}

void test_get_job_log_dir() {
  char *logdir = (char *) get_job_log_dir("/tmp/testing",
    "job_200906101234_0001");
  printf("logdir obtained is %s\n", logdir);
  int ret = 0;
  if (strcmp(logdir, "/tmp/testing/userlogs/job_200906101234_0001") != 0) {
    ret = -1;
  }
  free(logdir);
  assert(ret == 0);
}

void test_get_job_acls_file() {
  char *job_acls_file = (char *) get_job_acls_file(
    "/tmp/testing/userlogs/job_200906101234_0001");
  printf("job acls file obtained is %s\n", job_acls_file);
  int ret = 0;
  if (strcmp(job_acls_file,
    "/tmp/testing/userlogs/job_200906101234_0001/job-acls.xml") != 0) {
    ret = -1;
  }
  free(job_acls_file);
  assert(ret == 0);
}

void test_get_task_log_dir() {
  char *logdir = (char *) get_task_log_dir("/tmp/testing",
    "job_200906101234_0001", "attempt_200906112028_0001_m_000000_0");
  printf("logdir obtained is %s\n", logdir);
  int ret = 0;
  if (strcmp(logdir,
      "/tmp/testing/userlogs/job_200906101234_0001/attempt_200906112028_0001_m_000000_0")
      != 0) {
    ret = -1;
  }
  free(logdir);
  assert(ret == 0);
}

int main(int argc, char **argv) {
  printf("\nStarting tests\n");
  LOGFILE = stdout;

  printf("\nTesting check_variable_against_config()\n");
  test_check_variable_against_config();

  printf("\nTesting get_user_directory()\n");
  test_get_user_directory();

  printf("\nTesting get_job_directory()\n");
  test_get_job_directory();

  printf("\nTesting get_attempt_directory()\n");
  test_get_attempt_directory();

  printf("\nTesting get_task_launcher_file()\n");
  test_get_task_launcher_file();

  printf("\nTesting get_job_log_dir()\n");
  test_get_job_log_dir();

  printf("\nTesting get_job_acls_file()\n");
  test_get_job_acls_file();

  printf("\nTesting get_task_log_dir()\n");
  test_get_task_log_dir();

  printf("\nFinished tests\n");
  return 0;
}
