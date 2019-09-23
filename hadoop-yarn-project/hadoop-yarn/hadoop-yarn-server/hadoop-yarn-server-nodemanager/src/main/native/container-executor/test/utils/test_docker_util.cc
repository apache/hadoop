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

#include <gtest/gtest.h>
#include <fstream>
#include "errno.h"

extern "C" {
#include "utils/docker-util.c"
}

namespace ContainerExecutor {

  class TestDockerUtil : public ::testing::Test {
  protected:
    virtual void SetUp() {
      docker_command_file = "docker-command.cmd";
      container_executor_cfg_file = "container-executor.cfg";
      container_executor_cfg.size = 0;
      container_executor_cfg.sections = NULL;
    }

    virtual void TearDown() {
      remove(docker_command_file.c_str());
      remove(container_executor_cfg_file.c_str());
      delete_ce_file();
    }

    struct configuration container_executor_cfg;
    std::string docker_command_file;
    std::string container_executor_cfg_file;


    void write_file(const std::string fname, const std::string contents) {
      std::ofstream command_file;
      command_file.open(fname.c_str());
      command_file << contents;
      command_file.close();
    }

    int create_ce_file() {
      int ret = 0;
      const char *fname = HADOOP_CONF_DIR "/" CONF_FILENAME;
      if (strcmp("../etc/hadoop/container-executor.cfg", fname) == 0) {
        ret = mkdir("../etc", 0755);
        if (ret == 0 || errno == EEXIST) {
          ret = mkdir("../etc/hadoop", 0755);
          if (ret == 0 || errno == EEXIST) {
            write_file("../etc/hadoop/container-executor.cfg", "");
            return 0;
          } else {
            std::cerr << "Could not create ../etc/hadoop, " << strerror(errno) << std::endl;
          }
        } else {
          std::cerr << "Could not create ../etc, " << strerror(errno) << std::endl;
        }
      }
      std::cerr << "Could not create " << fname << std::endl;
      return 1;
    }

    void delete_ce_file() {
      const char *fname = HADOOP_CONF_DIR "/" CONF_FILENAME;
      if (strcmp("../etc/hadoop/container-executor.cfg", fname) == 0) {
        struct stat buffer;
        if (stat(fname, &buffer) == 0) {
          remove("../etc/hadoop/container-executor.cfg");
          rmdir("../etc/hadoop");
          rmdir("../etc");
        }
      }
    }

    void write_container_executor_cfg(const std::string contents) {
      write_file(container_executor_cfg_file, contents);
    }

    void write_command_file(const std::string contents) {
      write_file(docker_command_file, contents);
    }

    char* flatten(args *args) {
      size_t string_len = 0;
      size_t current_len = 0;
      char *buffer = (char *) malloc(8192 * sizeof(char));
      for (int i = 0; i < args->length; i++) {
        string_len = strlen(args->data[i]);
        if (string_len != 0) {
          strncpy(buffer + current_len, args->data[i], string_len);
          current_len = current_len + string_len;
          if (args->length - 1 != i) {
            strncpy(buffer + current_len, " ", 1);
            current_len++;
          }
        }
      }
      buffer[current_len] = '\0';
      return buffer;
    }

    void run_docker_command_test(const std::vector<std::pair<std::string, std::string> > &file_cmd_vec,
                                 const std::vector<std::pair<std::string, int> > &bad_file_cmd_vec,
                                 int (*docker_func)(const char *, const struct configuration *, args *)) {
      struct args tmp = ARGS_INITIAL_VALUE;
      std::vector<std::pair<std::string, std::string> >::const_iterator itr;
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        write_command_file(itr->first);
        int ret = (*docker_func)(docker_command_file.c_str(), &container_executor_cfg, &tmp);
        ASSERT_EQ(0, ret) << "error message: " << get_docker_error_message(ret) << " for input " << itr->first;
        char *actual = flatten(&tmp);
        ASSERT_STREQ(itr->second.c_str(), actual);
        reset_args(&tmp);
        free(actual);
      }

      std::vector<std::pair<std::string, int> >::const_iterator itr2;
      for (itr2 = bad_file_cmd_vec.begin(); itr2 != bad_file_cmd_vec.end(); ++itr2) {
        write_command_file(itr2->first);
        int ret = (*docker_func)(docker_command_file.c_str(), &container_executor_cfg, &tmp);
        ASSERT_EQ(itr2->second, ret) << " for " << itr2->first << std::endl;
        reset_args(&tmp);
      }
      int ret = (*docker_func)("unknown-file", &container_executor_cfg, &tmp);
      ASSERT_EQ(static_cast<int>(INVALID_COMMAND_FILE), ret);
      reset_args(&tmp);
    }

    void run_docker_run_helper_function(const std::vector<std::pair<std::string, std::string> > &file_cmd_vec,
                                        int (*helper_func)(const struct configuration *, args *)) {
      std::vector<std::pair<std::string, std::string> >::const_iterator itr;
      for(itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        struct configuration cfg;
        struct args buff = ARGS_INITIAL_VALUE;
        write_command_file(itr->first);
        int ret = read_config(docker_command_file.c_str(), &cfg);
        if(ret == 0) {
          ret = (*helper_func)(&cfg, &buff);
          char *actual = flatten(&buff);
          ASSERT_EQ(0, ret);
          ASSERT_STREQ(itr->second.c_str(), actual);
          reset_args(&buff);
          free(actual);
          free_configuration(&cfg);
        }
      }
    }
  };

  TEST_F(TestDockerUtil, test_docker_inspect) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=inspect\n  format={{.State.Status}}\n  name=container_e1_12312_11111_02_000001",
        "inspect --format={{.State.Status}} container_e1_12312_11111_02_000001"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=inspect\n"
            "  format={{range(.NetworkSettings.Networks)}}{{.IPAddress}},{{end}}{{.Config.Hostname}}\n"
            "  name=container_e1_12312_11111_02_000001",
        "inspect --format={{range(.NetworkSettings.Networks)}}{{.IPAddress}},{{end}}{{.Config.Hostname}} container_e1_12312_11111_02_000001"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=inspect\n  format={{json .NetworkSettings.Ports}}\n  name=container_e1_12312_11111_02_000001",
        "inspect --format={{json .NetworkSettings.Ports}} container_e1_12312_11111_02_000001"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=inspect\n  format={{.State.Status}},{{.Config.StopSignal}}\n  name=container_e1_12312_11111_02_000001",
        "inspect --format={{.State.Status}},{{.Config.StopSignal}} container_e1_12312_11111_02_000001"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  format='{{.State.Status}}'",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=inspect\n  format='{{.State.Status}}'",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n  format={{.State.Status}}\n  name=",
        static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n  format={{.State.Status}}",
        static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n  format=\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INVALID_DOCKER_INSPECT_FORMAT)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INVALID_DOCKER_INSPECT_FORMAT)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n format={{.IPAddress}}\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INVALID_DOCKER_INSPECT_FORMAT)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n format={{.NetworkSettings.Ports}}\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INVALID_DOCKER_INSPECT_FORMAT)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=inspect\n format={{.Config.StopSignal}}\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INVALID_DOCKER_INSPECT_FORMAT)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_inspect_command);
  }

  TEST_F(TestDockerUtil, test_docker_load) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=load\n  image=image-id",
        "load --i=image-id"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=image-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=load\n  image=image-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=load\n  image=", static_cast<int>(INVALID_DOCKER_IMAGE_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>("[docker-command-execution]\n  docker-command=load",
                                                                static_cast<int>(INVALID_DOCKER_IMAGE_NAME)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_load_command);
  }

  TEST_F(TestDockerUtil, test_docker_validate_image_name) {
    const char *good_input[] = {
        "ubuntu",
        "ubuntu:latest",
        "ubuntu:14.04",
        "ubuntu:LATEST",
        "registry.com:5000/user/ubuntu",
        "registry.com:5000/user/ubuntu:latest",
        "registry.com:5000/user/ubuntu:0.1.2.3",
        "registry.com/user/ubuntu",
        "registry.com/user/ubuntu:latest",
        "registry.com/user/ubuntu:0.1.2.3",
        "registry.com/user/ubuntu:test-image",
        "registry.com/user/ubuntu:test_image",
        "registry.com/ubuntu",
        "user/ubuntu",
        "user/ubuntu:0.1.2.3",
        "user/ubuntu:latest",
        "user/ubuntu:test_image",
        "user/ubuntu.test:test_image",
        "user/ubuntu-test:test-image",
        "registry.com/ubuntu/ubuntu/ubuntu"
    };

    const char *bad_input[] = {
        "UBUNTU",
        "registry.com|5000/user/ubuntu",
        "registry.com | 5000/user/ubuntu",
        "ubuntu' || touch /tmp/file #",
        "ubuntu || touch /tmp/file #",
        "''''''''",
        "bad_host_name:5000/user/ubuntu",
        "registry.com:foo/ubuntu/ubuntu/ubuntu",
        "registry.com/ubuntu:foo/ubuntu/ubuntu"
    };

    int good_input_size = sizeof(good_input) / sizeof(char *);
    int i = 0;
    for (i = 0; i < good_input_size; i++) {
      int op = validate_docker_image_name(good_input[i]);
      ASSERT_EQ(0, op);
    }

    int bad_input_size = sizeof(bad_input) / sizeof(char *);
    int j = 0;
    for (j = 0; j < bad_input_size; j++) {
      int op = validate_docker_image_name(bad_input[j]);
      ASSERT_EQ(1, op);
    }
  }

  TEST_F(TestDockerUtil, test_docker_pull) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=pull\n  image=image-id",
        "pull image-id"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=image-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=pull\n  image=image-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=pull\n  image=", static_cast<int>(INVALID_DOCKER_IMAGE_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>("[docker-command-execution]\n  docker-command=pull",
                                                                static_cast<int>(INVALID_DOCKER_IMAGE_NAME)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_pull_command);
  }

  TEST_F(TestDockerUtil, test_docker_rm) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(
        std::make_pair<std::string, std::string>(
            "[docker-command-execution]\n  docker-command=rm\n  name=container_e1_12312_11111_02_000001",
            "rm -f container_e1_12312_11111_02_000001"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=rm\n  name=ctr-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=rm\n  name=", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=rm", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_rm_command);
  }

  TEST_F(TestDockerUtil, test_docker_stop) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=stop\n  name=container_e1_12312_11111_02_000001",
        "stop container_e1_12312_11111_02_000001"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=stop\n  name=container_e1_12312_11111_02_000001\ntime=25",
        "stop --time=25 container_e1_12312_11111_02_000001"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=stop\n  name=ctr-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=stop\n  name=", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=stop", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=stop\n  name=container_e1_12312_11111_02_000001\n  time=abcd",
        static_cast<int>(INVALID_DOCKER_STOP_COMMAND)));


    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_stop_command);
  }

  TEST_F(TestDockerUtil, test_docker_kill) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=kill\n  name=container_e1_12312_11111_02_000001",
        "kill container_e1_12312_11111_02_000001"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=kill\n  name=container_e1_12312_11111_02_000001\nsignal=SIGQUIT",
        "kill --signal=SIGQUIT container_e1_12312_11111_02_000001"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=kill\n  name=ctr-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=kill\n  name=", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=kill", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=kill\n  name=container_e1_12312_11111_02_000001\n  signal=foo | bar",
        static_cast<int>(INVALID_DOCKER_KILL_COMMAND)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_kill_command);
  }

  TEST_F(TestDockerUtil, test_docker_start) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
         "[docker-command-execution]\n  docker-command=start\n  name=container_e1_12312_11111_02_000001",
         "start container_e1_12312_11111_02_000001"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=start\n  name=ctr-id", static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=start\n  name=", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=start", static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_start_command);
  }

  TEST_F(TestDockerUtil, test_detach_container) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  detach=true", "-d"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, detach_container);
  }

  TEST_F(TestDockerUtil, test_rm_container) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  rm=true", "--rm"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, rm_container_on_exit);
  }

  TEST_F(TestDockerUtil, test_set_container_workdir) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  workdir=/tmp/test", "--workdir=/tmp/test"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_container_workdir);
  }

  TEST_F(TestDockerUtil, test_set_cgroup_parent) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  cgroup-parent=/sys/fs/cgroup/yarn",
        "--cgroup-parent=/sys/fs/cgroup/yarn"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_cgroup_parent);
  }

  TEST_F(TestDockerUtil, test_set_hostname) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  hostname=ctr-id", "--hostname=ctr-id"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_hostname);
  }

  TEST_F(TestDockerUtil, test_set_runtime) {
    struct configuration container_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n"
        "  docker.trusted.registries=hadoop\n"
        "  docker.allowed.runtimes=lxc,nvidia";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n runtime=lxc", "--runtime=lxc"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n runtime=nvidia", "--runtime=nvidia"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      struct configuration cmd_cfg;
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_runtime(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret) << "error message: " << get_docker_error_message(ret) << " for input " << itr->first;
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
    struct configuration cmd_cfg_1;
    write_command_file("[docker-command-execution]\n  docker-command=run\n  runtime=nvidia1");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg_1);
    if (ret != 0) {
      FAIL();
    }
    ret = set_runtime(&cmd_cfg_1, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_RUNTIME, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&container_cfg);

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_runtime(&cmd_cfg_1, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_RUNTIME, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg_1);
    free_configuration(&container_cfg);
  }

  TEST_F(TestDockerUtil, test_set_group_add) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  group-add=1000,1001", "--group-add 1000 --group-add 1001"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_group_add);
  }

  TEST_F(TestDockerUtil, test_set_network) {
    struct configuration container_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.allowed.networks=sdn1,bridge";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  net=bridge", "--net=bridge"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  net=sdn1", "--net=sdn1"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      struct configuration cmd_cfg;
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_network(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
    struct configuration cmd_cfg_1;
    write_command_file("[docker-command-execution]\n  docker-command=run\n  net=sdn2");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg_1);
    if (ret != 0) {
      FAIL();
    }
    ret = set_network(&cmd_cfg_1, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_NETWORK, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&container_cfg);

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_network(&cmd_cfg_1, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_NETWORK, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg_1);
    free_configuration(&container_cfg);
  }

  TEST_F(TestDockerUtil, test_add_ports_mapping_to_command) {
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ports-mapping=127.0.0.1:8080:80,1234:1234,:2222",
        "-p 127.0.0.1:8080:80 -p 1234:1234 -p :2222"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ports-mapping=1234:1234,:2222",
        "-p 1234:1234 -p :2222"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ports-mapping=:2222", "-p :2222"));

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      struct configuration cmd_cfg;
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_ports_mapping_to_command(&cmd_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret) << "error message: " << get_docker_error_message(ret) << " for input " << itr->first;
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
    struct configuration cmd_cfg_1;
    write_command_file("[docker-command-execution]\n  docker-command=run\n  ports-mapping=327.0.0.1:8080:80,1234:1234,:2222");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg_1);
    if (ret != 0) {
      FAIL();
    }
    ret = add_ports_mapping_to_command(&cmd_cfg_1, &buff);
    ASSERT_EQ(INVALID_DOCKER_PORTS_MAPPING, ret);
    reset_args(&buff);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  ports-mapping=127.0.0.1:8080:80,12s4:1234,:2222");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg_1);
    if (ret != 0) {
      FAIL();
    }
    ret = add_ports_mapping_to_command(&cmd_cfg_1, &buff);
    ASSERT_EQ(INVALID_DOCKER_PORTS_MAPPING, ret);
    reset_args(&buff);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  ports-mapping=127.0.0.1:8080:80,1234:1234,:s2s2");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg_1);
    if (ret != 0) {
      FAIL();
    }
    ret = add_ports_mapping_to_command(&cmd_cfg_1, &buff);
    ASSERT_EQ(INVALID_DOCKER_PORTS_MAPPING, ret);
    reset_args(&buff);
    free_configuration(&cmd_cfg_1);
  }

  TEST_F(TestDockerUtil, test_set_pid_namespace) {
    struct configuration container_cfg, cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents[] = {"[docker]\n  docker.host-pid-namespace.enabled=1",
                                                     "[docker]\n  docker.host-pid-namespace.enabled=true",
                                                     "[docker]\n  docker.host-pid-namespace.enabled=True",
                                                     "[docker]\n  docker.host-pid-namespace.enabled=0",
                                                     "[docker]\n  docker.host-pid-namespace.enabled=false",
                                                     "[docker]\n"};
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    std::vector<std::pair<std::string, int> >::const_iterator itr2;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  pid=host", "--pid=host"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n pid=other",
        static_cast<int>(INVALID_PID_NAMESPACE)));

    for (int i = 1; i < 3; ++i) {
      write_container_executor_cfg(container_executor_cfg_contents[0]);
      ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

      if (ret != 0) {
        FAIL();
      }
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        write_command_file(itr->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_pid_namespace(&cmd_cfg, &container_cfg, &buff);
        char *actual = flatten(&buff);
        ASSERT_EQ(0, ret);
        ASSERT_STREQ(itr->second.c_str(), actual);
        reset_args(&buff);
        free(actual);
        free_configuration(&cmd_cfg);
      }
      for (itr2 = bad_file_cmd_vec.begin(); itr2 != bad_file_cmd_vec.end(); ++itr2) {
        write_command_file(itr2->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_pid_namespace(&cmd_cfg, &container_cfg, &buff);
        ASSERT_EQ(itr2->second, ret);
        ASSERT_EQ(0, buff.length);
        reset_args(&buff);
        free_configuration(&cmd_cfg);
      }
      free_configuration(&container_cfg);
    }

    // check default case and when it's turned off
    for (int i = 3; i < 6; ++i) {
      write_container_executor_cfg(container_executor_cfg_contents[i]);
      ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
      if (ret != 0) {
        FAIL();
      }
      file_cmd_vec.clear();
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run", ""));
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        write_command_file(itr->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_pid_namespace(&cmd_cfg, &container_cfg, &buff);
        char *actual = flatten(&buff);
        ASSERT_EQ(0, ret);
        ASSERT_STREQ(itr->second.c_str(), actual);
        free(actual);
        free_configuration(&cmd_cfg);
      }
      bad_file_cmd_vec.clear();
      bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n pid=other",
        static_cast<int>(INVALID_PID_NAMESPACE)));
      bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n pid=host",
        static_cast<int>(PID_HOST_DISABLED)));
      for (itr2 = bad_file_cmd_vec.begin(); itr2 != bad_file_cmd_vec.end(); ++itr2) {
        write_command_file(itr2->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_pid_namespace(&cmd_cfg, &container_cfg, &buff);
        ASSERT_EQ(itr2->second, ret);
        ASSERT_EQ(0, buff.length);
        reset_args(&buff);
        free_configuration(&cmd_cfg);
      }
      free_configuration(&container_cfg);
    }
  }

  TEST_F(TestDockerUtil, test_check_mount_permitted) {
    const char *permitted_mounts[] = {"/etc", "/usr/bin/cut", "/tmp/", "regex:nvidia_driver.*", NULL};
    std::vector<std::pair<std::string, int> > test_data;
    test_data.push_back(std::make_pair<std::string, int>("/etc", 1));
    test_data.push_back(std::make_pair<std::string, int>("/etc/", 1));
    test_data.push_back(std::make_pair<std::string, int>("/etc/passwd", 1));
    test_data.push_back(std::make_pair<std::string, int>("/usr/bin/cut", 1));
    test_data.push_back(std::make_pair<std::string, int>("//usr/", 0));
    test_data.push_back(std::make_pair<std::string, int>("nvidia_driver_375.66", 1));
    test_data.push_back(std::make_pair<std::string, int>("nvidia_local_driver", 0));
    test_data.push_back(std::make_pair<std::string, int>("^/usr/.*$", -1));
    test_data.push_back(std::make_pair<std::string, int>("/etc/random-file", -1));

    std::vector<std::pair<std::string, int> >::const_iterator itr;
    for (itr = test_data.begin(); itr != test_data.end(); ++itr) {
      int ret = check_mount_permitted(permitted_mounts, itr->first.c_str());
      ASSERT_EQ(itr->second, ret) << "for input " << itr->first;
    }
  }

  TEST_F(TestDockerUtil, test_normalize_mounts) {
    const int entries = 5;
    const char *permitted_mounts[] = {"/home", "/etc", "/usr/bin/cut", "regex:/dev/nvidia.*", NULL};
    const char *expected[] = {"/home/", "/etc/", "/usr/bin/cut", "regex:/dev/nvidia.*", NULL};
    char **ptr = static_cast<char **>(malloc(entries * sizeof(char *)));
    for (int i = 0; i < entries; ++i) {
      if (permitted_mounts[i] != NULL) {
        ptr[i] = strdup(permitted_mounts[i]);
      } else {
        ptr[i] = NULL;
      }
    }
    normalize_mounts(ptr, 1);
    for (int i = 0; i < entries; ++i) {
      ASSERT_STREQ(expected[i], ptr[i]);
    }
    free_values(ptr);
  }

  TEST_F(TestDockerUtil, test_set_privileged) {
    struct configuration container_cfg, cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents[] = {"[docker]\n  docker.privileged-containers.enabled=1\n"
                                                     "  docker.trusted.registries=library\n"
                                                     "  docker.privileged-containers.registries=hadoop",
                                                     "[docker]\n  docker.privileged-containers.enabled=true\n  docker.trusted.registries=hadoop",
                                                     "[docker]\n  docker.privileged-containers.enabled=True\n  docker.trusted.registries=hadoop",
                                                     "[docker]\n  docker.privileged-containers.enabled=0",
                                                     "[docker]\n  docker.privileged-containers.enabled=false",
                                                     "[docker]\n"};
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  privileged=true\n  image=hadoop/image\n  use-entry-point=true", "--privileged "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  privileged=false\n image=hadoop/image", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  privileged=false\n image=nothadoop/image", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  privileged=false", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));
    for (int i = 0; i < 3; i++ ) {
      write_container_executor_cfg(container_executor_cfg_contents[i]);
      ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

      if (ret != 0) {
        FAIL();
      }
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        write_command_file(itr->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_privileged(&cmd_cfg, &container_cfg, &buff);
        ASSERT_EQ(6, ret);
        ASSERT_EQ(0, buff.length);
        reset_args(&buff);
        free_configuration(&cmd_cfg);
      }
      write_command_file("[docker-command-execution]\n docker-command=run\n  user=nobody\n privileged=true\n image=nothadoop/image");
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_privileged(&cmd_cfg, &container_cfg, &buff);
      ASSERT_EQ(PRIVILEGED_CONTAINERS_DISABLED, ret);
      ASSERT_EQ(0, buff.length);
      reset_args(&buff);
      free_configuration(&cmd_cfg);
      free_configuration(&container_cfg);
    }

    // check default case and when it's turned off
    for (int i = 3; i < 6; ++i) {
      write_container_executor_cfg(container_executor_cfg_contents[i]);
      ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
      if (ret != 0) {
        FAIL();
      }
      file_cmd_vec.clear();
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n  user=root\n privileged=false", ""));
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        write_command_file(itr->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_privileged(&cmd_cfg, &container_cfg, &buff);
        char *actual = flatten(&buff);
        ASSERT_EQ(0, ret);
        ASSERT_STREQ(itr->second.c_str(), actual);
        reset_args(&buff);
        free(actual);
        free_configuration(&cmd_cfg);
      }
      write_command_file("[docker-command-execution]\n  docker-command=run\n  user=root\n privileged=true");
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_privileged(&cmd_cfg, &container_cfg, &buff);
      ASSERT_EQ(PRIVILEGED_CONTAINERS_DISABLED, ret);
      ASSERT_EQ(0, buff.length);
      reset_args(&buff);
      free_configuration(&cmd_cfg);
      free_configuration(&container_cfg);
    }
  }

  TEST_F(TestDockerUtil, test_set_capabilities) {
    struct configuration container_cfg, cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n"
        "  docker.allowed.capabilities=CHROOT,MKNOD\n"
        "  docker.trusted.registries=hadoop\n";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/docker-image\n  cap-add=CHROOT,MKNOD",
        "--cap-drop=ALL --cap-add=CHROOT --cap-add=MKNOD"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/docker-image\n  cap-add=CHROOT,MKNOD",
        "--cap-drop=ALL"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/docker-image\n  cap-add=CHROOT",
        "--cap-drop=ALL --cap-add=CHROOT"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/docker-image\n",
        "--cap-drop=ALL"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/docker-image\n",
        "--cap-drop=ALL"));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_capabilities(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/docker-image\n  cap-add=SETGID");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_capabilities(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_CAPABILITY, ret);
    reset_args(&buff);
    free_configuration(&container_cfg);

    container_executor_cfg_contents = "[docker]\n  docker.trusted.registries=hadoop\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_capabilities(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_CAPABILITY, ret);
    reset_args(&buff);
    free_configuration(&cmd_cfg);
    free_configuration(&container_cfg);
  }

  TEST_F(TestDockerUtil, test_set_devices) {
    struct configuration container_cfg, cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n"
        "  docker.trusted.registries=hadoop\n"
        "  docker.allowed.devices=/dev/test-device,/dev/device2,regex:/dev/nvidia.*,regex:/dev/gpu-uvm.*";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/test-device:/dev/test-device",
        "--device=/dev/test-device:/dev/test-device"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/device2:/dev/device2",
        "--device=/dev/device2:/dev/device2"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n"
            "  devices=/dev/test-device:/dev/test-device,/dev/device2:/dev/device2",
        "--device=/dev/test-device:/dev/test-device --device=/dev/device2:/dev/device2"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n"
            "devices=/dev/nvidiactl:/dev/nvidiactl",
        "--device=/dev/nvidiactl:/dev/nvidiactl"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n"
            "devices=/dev/nvidia1:/dev/nvidia1,/dev/gpu-uvm-tools:/dev/gpu-uvm-tools",
        "--device=/dev/nvidia1:/dev/nvidia1 --device=/dev/gpu-uvm-tools:/dev/gpu-uvm-tools"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image", ""));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_devices(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
    write_command_file("[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n  devices=/dev/test-device:/dev/test-device");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/device3:/dev/device3");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/device1");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/testnvidia:/dev/testnvidia");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/gpu-nvidia-uvm:/dev/gpu-nvidia-uvm");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);

    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  devices=/dev/device1");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&container_cfg);

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = set_devices(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);
    free_configuration(&container_cfg);
  }


  TEST_F(TestDockerUtil, test_add_mounts) {
    struct configuration container_cfg, cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.trusted.registries=hadoop\n  "
                                                              "docker.allowed.rw-mounts=/opt,/var,/usr/bin/cut,/usr/bin/awk\n  "
                                                              "docker.allowed.ro-mounts=/etc/passwd";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n  mounts=/var:/var:rw", "-v /var:/var:rw"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n  mounts=/var:/var:rw", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/var/:/var/:rw", "-v /var/:/var/:rw"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/usr/bin/cut:/usr/bin/cut:rw",
        "-v /usr/bin/cut:/usr/bin/cut:rw"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n  mounts=/lib:/lib:rw",
        ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/opt:/mydisk1:rw,/var/log/:/mydisk2:rw",
        "-v /opt:/mydisk1:rw -v /var/log/:/mydisk2:rw"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n  mounts=/opt:/mydisk1:rw,/var/log/:/mydisk2:rw",
        ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n mounts=/usr/bin/awk:/awk:rw+shared,/etc/passwd:/etc/passwd:ro",
        "-v /usr/bin/awk:/awk:rw,shared -v /etc/passwd:/etc/passwd:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n mounts=/var:/var:ro+rprivate,/etc/passwd:/etc/passwd:ro+rshared",
        "-v /var:/var:ro,rprivate -v /etc/passwd:/etc/passwd:ro,rshared"));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }

    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;

    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }

    std::vector<std::pair<std::string, int> > bad_file_cmds_vec;
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/lib:/lib:rw",
        static_cast<int>(INVALID_DOCKER_RW_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/usr/bin/:/usr/bin:rw",
        static_cast<int>(INVALID_DOCKER_RW_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/blah:/blah:rw",
        static_cast<int>(INVALID_DOCKER_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n mounts=/tmp:/tmp:shared",
        static_cast<int>(INVALID_DOCKER_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n mounts=/lib:/lib",
        static_cast<int>(INVALID_DOCKER_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n mounts=/lib:/lib:other",
        static_cast<int>(INVALID_DOCKER_MOUNT)));

    std::vector<std::pair<std::string, int> >::const_iterator itr2;

    for (itr2 = bad_file_cmds_vec.begin(); itr2 != bad_file_cmds_vec.end(); ++itr2) {
      write_command_file(itr2->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(itr2->second, ret);
      ASSERT_STREQ("", actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }

    // verify that you can't mount any directory in the container-executor.cfg path
    char *ce_path = realpath("../etc/hadoop/container-executor.cfg", NULL);
    while (strlen(ce_path) != 0) {
      std::string cmd_file_contents = "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=";
      cmd_file_contents.append(ce_path).append(":").append("/etc/hadoop").append(":rw");
      write_command_file(cmd_file_contents);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
      ASSERT_EQ(INVALID_DOCKER_RW_MOUNT, ret) << " for input " << cmd_file_contents;
      char *actual = flatten(&buff);
      ASSERT_STREQ("", actual);
      reset_args(&buff);
      free_configuration(&cmd_cfg);
      free(actual);
      char *tmp = strrchr(ce_path, '/');
      if (tmp != NULL) {
        *tmp = '\0';
      }
    }
    free(ce_path);
    free_configuration(&container_cfg);

    // For untrusted image, container add_mounts will pass through
    // without mounting or report error code.
    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
    char *actual = flatten(&buff);
    ASSERT_EQ(0, ret);
    ASSERT_STREQ("", actual);
    reset_args(&buff);
    free(actual);
    free_configuration(&container_cfg);
  }

  TEST_F(TestDockerUtil, test_add_ro_mounts) {
    struct configuration container_cfg, cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;

    std::string container_executor_cfg_contents = "[docker]\n  docker.trusted.registries=hadoop\n  "
                                                              "docker.allowed.rw-mounts=/home/,/var,/usr/bin/cut\n  "
                                                              "docker.allowed.ro-mounts=/etc/passwd,/etc/group";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n  mounts=/var:/var:ro", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=nothadoop/image\n  mounts=/etc:/etc:ro", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/var/:/var/:ro", "-v /var/:/var/:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/home:/home:ro", "-v /home:/home:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/home/:/home:ro", "-v /home/:/home:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/usr/bin/cut:/usr/bin/cut:ro",
        "-v /usr/bin/cut:/usr/bin/cut:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/etc/group:/etc/group:ro",
        "-v /etc/group:/etc/group:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/etc/passwd:/etc/passwd:ro",
        "-v /etc/passwd:/etc/passwd:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/var/log:/mydisk1:ro,/etc/passwd:/etc/passwd:ro",
        "-v /var/log:/mydisk1:ro -v /etc/passwd:/etc/passwd:ro"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n", ""));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }

    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;

    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }

    std::vector<std::pair<std::string, int> > bad_file_cmds_vec;
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/etc:/etc:ro",
        static_cast<int>(INVALID_DOCKER_RO_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/blah:/blah:ro",
        static_cast<int>(INVALID_DOCKER_MOUNT)));

    std::vector<std::pair<std::string, int> >::const_iterator itr2;

    for (itr2 = bad_file_cmds_vec.begin(); itr2 != bad_file_cmds_vec.end(); ++itr2) {
      write_command_file(itr2->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(itr2->second, ret);
      ASSERT_STREQ("", actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
    free_configuration(&container_cfg);

    container_executor_cfg_contents = "[docker]\n  docker.trusted.registries=hadoop\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    write_command_file("[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  mounts=/home:/home:ro");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = add_mounts(&cmd_cfg, &container_cfg, &buff);
    ASSERT_EQ(INVALID_DOCKER_RO_MOUNT, ret);
    ASSERT_EQ(0, buff.length);
    reset_args(&buff);
    free_configuration(&cmd_cfg);
    free_configuration(&container_cfg);
  }

  TEST_F(TestDockerUtil, test_add_tmpfs_mounts) {
    struct configuration cmd_cfg;
    struct args buff = ARGS_INITIAL_VALUE;
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.trusted.registries=hadoop\n";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n image=hadoop/image\n  tmpfs=/run",
        "--tmpfs /run"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  tmpfs=/run,/run2",
        "--tmpfs /run --tmpfs /run2"));
    write_container_executor_cfg(container_executor_cfg_contents);

    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_tmpfs_mounts(&cmd_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }

    std::vector<std::pair<std::string, int> > bad_file_cmds_vec;
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  tmpfs=run",
        static_cast<int>(INVALID_DOCKER_TMPFS_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  tmpfs=/ru:n",
        static_cast<int>(INVALID_DOCKER_TMPFS_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/image\n  tmpfs=/run:",
        static_cast<int>(INVALID_DOCKER_TMPFS_MOUNT)));

    std::vector<std::pair<std::string, int> >::const_iterator itr2;
    for (itr2 = bad_file_cmds_vec.begin(); itr2 != bad_file_cmds_vec.end(); ++itr2) {
      write_command_file(itr2->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_tmpfs_mounts(&cmd_cfg, &buff);
      char *actual = flatten(&buff);
      ASSERT_EQ(itr2->second, ret);
      ASSERT_STREQ("", actual);
      reset_args(&buff);
      free(actual);
      free_configuration(&cmd_cfg);
    }
  }

  TEST_F(TestDockerUtil, test_docker_run_privileged) {

    std::string container_executor_contents = "[docker]\n  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
        "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n "
        "  docker.privileged-containers.enabled=1\n  docker.allowed.capabilities=CHOWN,SETUID\n"
        "  docker.allowed.devices=/dev/test\n  docker.trusted.registries=hadoop\n";
    write_file(container_executor_cfg_file, container_executor_contents);
    int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }

    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL hadoop/docker-image"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n name=container_e1_12312_11111_02_000001\n image=nothadoop/docker-image\n  user=nobody",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL nothadoop/docker-image"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL hadoop/docker-image bash test_script.sh arg1 arg2"));

    // Test non-privileged conatiner with launch command
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm -v /var/log:/var/log:ro -v /var/lib:/lib:ro"
            " -v /usr/bin/cut:/usr/bin/cut:ro -v /tmp:/tmp:rw --cgroup-parent=ctr-cgroup --cap-drop=ALL --cap-add=CHOWN"
            " --cap-add=SETUID --hostname=host-id --device=/dev/test:/dev/test hadoop/docker-image bash "
            "test_script.sh arg1 arg2"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n image=nothadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm"
            " --cgroup-parent=ctr-cgroup --cap-drop=ALL --hostname=host-id nothadoop/docker-image bash test_script.sh arg1 arg2"));

    // Test non-privileged container and drop all privileges
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm -v /var/log:/var/log:ro -v /var/lib:/lib:ro"
            " -v /usr/bin/cut:/usr/bin/cut:ro -v /tmp:/tmp:rw --cgroup-parent=ctr-cgroup --cap-drop=ALL --cap-add=CHOWN "
            "--cap-add=SETUID --hostname=host-id --device=/dev/test:/dev/test hadoop/docker-image bash"
            " test_script.sh arg1 arg2"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n image=nothadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm"
            " --cgroup-parent=ctr-cgroup --cap-drop=ALL --hostname=host-id nothadoop/docker-image bash test_script.sh arg1 arg2"));

    // Test privileged container
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=root\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  privileged=true\n  use-entry-point=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 -d --rm -v /var/log:/var/log:ro -v /var/lib:/lib:ro"
            " -v /usr/bin/cut:/usr/bin/cut:ro -v /tmp:/tmp:rw --cgroup-parent=ctr-cgroup --privileged --cap-drop=ALL "
            "--cap-add=CHOWN --cap-add=SETUID --hostname=host-id --device=/dev/test:/dev/test hadoop/docker-image "
            "bash test_script.sh arg1 arg2"));

    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=root\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  privileged=true\n  use-entry-point=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n  group-add=1000,1001\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 -d --rm -v /var/log:/var/log:ro -v /var/lib:/lib:ro"
            " -v /usr/bin/cut:/usr/bin/cut:ro -v /tmp:/tmp:rw --cgroup-parent=ctr-cgroup --privileged --cap-drop=ALL "
            "--cap-add=CHOWN --cap-add=SETUID --hostname=host-id "
            "--device=/dev/test:/dev/test hadoop/docker-image bash test_script.sh arg1 arg2"));

    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=nobody\n  hostname=host-id\n"
            "  network=bridge\n  detach=true\n  rm=true\n  group-add=1000,1001\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm --cap-drop=ALL "
            "--hostname=host-id --group-add 1000 --group-add 1001 "
            "docker-image bash test_script.sh arg1 arg2"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;

    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=hadoop/docker-image\n  user=nobody",
        static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  user=nobody\n",
        static_cast<int>(INVALID_DOCKER_IMAGE_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n",
        static_cast<int>(INVALID_DOCKER_USER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n image=nothadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  privileged=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n  group-add=1000,1001\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        PRIVILEGED_CONTAINERS_DISABLED));

    // invalid rw mount
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/var/log:/var/log:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_RW_MOUNT)));

    // invalid ro mount
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/bin:/bin:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_RO_MOUNT)));

    // invalid capability
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID,SETGID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_CAPABILITY)));

    // invalid device
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/dev1:/dev/dev1\n  privileged=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(PRIVILEGED_CONTAINERS_DISABLED)));

    // invalid network
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
            "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  privileged=true\n  net=host\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_NETWORK)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
    free_configuration(&container_executor_cfg);
  }

  TEST_F(TestDockerUtil, test_docker_run_entry_point) {

    std::string container_executor_contents = "[docker]\n"
        "  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
        "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n "
        "  docker.privileged-containers.enabled=1\n  docker.allowed.capabilities=CHOWN,SETUID\n"
        "  docker.allowed.devices=/dev/test\n  docker.trusted.registries=hadoop\n";
    write_file(container_executor_cfg_file, container_executor_contents);
    int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }

    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
        "  docker-command=run\n"
        "  name=container_e1_12312_11111_02_000001\n"
        "  image=hadoop/docker-image\n"
        "  user=nobody\n"
        "  use-entry-point=true\n"
        "  environ=/tmp/test.env\n",
        "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL "
        "--env-file /tmp/test.env hadoop/docker-image"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;

    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
        "  docker-command=run\n"
        "  image=hadoop/docker-image\n"
        "  user=nobody",
        static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
    free_configuration(&container_executor_cfg);
  }

  TEST_F(TestDockerUtil, test_docker_run_no_privileged) {

    std::string container_executor_contents[] = {"[docker]\n  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n"
                                                     "  docker.allowed.capabilities=CHOWN,SETUID\n"
                                                     "  docker.allowed.devices=/dev/test",
                                                 "[docker]\n  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n"
                                                     "  docker.allowed.capabilities=CHOWN,SETUID\n"
                                                     "  privileged=0\n"
                                                     "  docker.allowed.devices=/dev/test"};
    for (int i = 0; i < 2; ++i) {
      write_file(container_executor_cfg_file, container_executor_contents[i]);
      int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = create_ce_file();
      if (ret != 0) {
        std::cerr << "Could not create ce file, skipping test" << std::endl;
        return;
      }

      std::vector<std::pair<std::string, std::string> > file_cmd_vec;
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=nobody",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL docker-image"));
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n"
              "  user=nobody\n  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL docker-image bash test_script.sh arg1 arg2"));

      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
              "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
              "  network=bridge\n  devices=/dev/test:/dev/test\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm -v /var/log:/var/log:ro -v /var/lib:/lib:ro"
              " -v /usr/bin/cut:/usr/bin/cut:ro -v /tmp:/tmp:rw --cgroup-parent=ctr-cgroup --cap-drop=ALL --cap-add=CHOWN"
              " --cap-add=SETUID --hostname=host-id --device=/dev/test:/dev/test hadoop/docker-image bash "
              "test_script.sh arg1 arg2"));
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n image=nothadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
              "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
              "  network=bridge\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm"
              " --cgroup-parent=ctr-cgroup --cap-drop=ALL --hostname=host-id nothadoop/docker-image bash test_script.sh arg1 arg2"));

      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
              "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
              "  network=bridge\n  devices=/dev/test:/dev/test\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm -v /var/log:/var/log:ro -v /var/lib:/lib:ro"
              " -v /usr/bin/cut:/usr/bin/cut:ro -v /tmp:/tmp:rw --cgroup-parent=ctr-cgroup --cap-drop=ALL --cap-add=CHOWN "
              "--cap-add=SETUID --hostname=host-id --device=/dev/test:/dev/test hadoop/docker-image bash"
              " test_script.sh arg1 arg2"));
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n image=nothadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
              "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
              "  network=bridge\n  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody -d --rm"
              " --cgroup-parent=ctr-cgroup --cap-drop=ALL --hostname=host-id nothadoop/docker-image bash test_script.sh arg1 arg2"));

      std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
      bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=nobody\n  hostname=host-id\n"
              "  mounts=/var/log:/var/log:ro,/var/lib:/lib:ro,/usr/bin/cut:/usr/bin/cut:ro,/tmp:/tmp:rw\n"
              "  network=bridge\n  devices=/dev/test:/dev/test\n  privileged=true\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          static_cast<int>(PRIVILEGED_CONTAINERS_DISABLED)));

      run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
      free_configuration(&container_executor_cfg);
    }
  }

  TEST_F(TestDockerUtil, test_docker_config_param) {
    std::vector<std::pair<std::string, std::string> > input_output_map;
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=inspect\n  docker-config=/my-config\n"
            "  format={{.State.Status}}\n  name=container_e1_12312_11111_02_000001",
        "/usr/bin/docker --config=/my-config inspect --format={{.State.Status}} container_e1_12312_11111_02_000001"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=load\n  docker-config=/my-config\n  image=image-id",
        "/usr/bin/docker --config=/my-config load --i=image-id"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=pull\n  docker-config=/my-config\n  image=image-id",
        "/usr/bin/docker --config=/my-config pull image-id"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=rm\n  docker-config=/my-config\n  name=container_e1_12312_11111_02_000001",
        "/usr/bin/docker --config=/my-config rm -f container_e1_12312_11111_02_000001"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=stop\n  docker-config=/my-config\n  name=container_e1_12312_11111_02_000001",
        "/usr/bin/docker --config=/my-config stop container_e1_12312_11111_02_000001"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  docker-config=/my-config\n  name=container_e1_12312_11111_02_000001\n"
            "  image=docker-image\n  user=nobody",
        "/usr/bin/docker --config=/my-config run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL docker-image"));

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    struct args buffer = ARGS_INITIAL_VALUE;
    struct configuration cfg = {0, NULL};
    for (itr = input_output_map.begin(); itr != input_output_map.end(); ++itr) {
      write_command_file(itr->first);
      int ret = get_docker_command(docker_command_file.c_str(), &cfg, &buffer);
      char *actual = flatten(&buffer);
      ASSERT_EQ(0, ret) << "for input " << itr->first;
      ASSERT_STREQ(itr->second.c_str(), actual);
      reset_args(&buffer);
      free(actual);
    }
  }

  TEST_F(TestDockerUtil, test_docker_module_enabled) {

    std::vector<std::pair<std::string, int> > input_out_vec;
    input_out_vec.push_back(std::make_pair<std::string, int>("[docker]\n  module.enabled=true", 1));
    input_out_vec.push_back(std::make_pair<std::string, int>("[docker]\n  module.enabled=false", 0));
    input_out_vec.push_back(std::make_pair<std::string, int>("[docker]\n  module.enabled=1", 0));
    input_out_vec.push_back(std::make_pair<std::string, int>("[docker]\n", 0));

    for (size_t i = 0; i < input_out_vec.size(); ++i) {
      write_file(container_executor_cfg_file, input_out_vec[i].first);
      int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = docker_module_enabled(&container_executor_cfg);
      ASSERT_EQ(input_out_vec[i].second, ret) << " incorrect output for "
                                              << input_out_vec[i].first;
      free_configuration(&container_executor_cfg);
    }
  }

  TEST_F(TestDockerUtil, test_docker_volume_command) {
    std::string container_executor_contents = "[docker]\n  docker.allowed.volume-drivers=driver1\n";
    write_file(container_executor_cfg_file, container_executor_contents);
    int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
    if (ret != 0) {
      FAIL();
    }

    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=create\n  volume=volume1 \n driver=driver1",
        "volume create --name=volume1 --driver=driver1"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
       "[docker-command-execution]\n  docker-command=volume\n  format={{.Name}},{{.Driver}}\n  sub-command=ls",
       "volume ls --format={{.Name}},{{.Driver}}"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;

    // Wrong subcommand
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=inspect\n  volume=volume1 \n driver=driver1",
        static_cast<int>(INVALID_DOCKER_VOLUME_COMMAND)));

    // Volume not specified
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=create\n  driver=driver1",
        static_cast<int>(INVALID_DOCKER_VOLUME_NAME)));

    // Invalid volume name
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=create\n  volume=/a/b/c \n driver=driver1",
        static_cast<int>(INVALID_DOCKER_VOLUME_NAME)));

    // Driver not specified
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=create\n  volume=volume1 \n",
        static_cast<int>(INVALID_DOCKER_VOLUME_DRIVER)));

    // Invalid driver name
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=create\n volume=volume1 \n driver=driver2",
        static_cast<int>(INVALID_DOCKER_VOLUME_DRIVER)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_volume_command);
    free_configuration(&container_executor_cfg);
  }

  TEST_F(TestDockerUtil, test_docker_no_new_privileges) {

    std::string container_executor_contents[] = {"[docker]\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.privileged-containers.enabled=false\n"
                                                     "  docker.no-new-privileges.enabled=true",
                                                 "[docker]\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.privileged-containers.enabled=true\n"
                                                     "  docker.no-new-privileges.enabled=true",
                                                 "[docker]\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.privileged-containers.enabled=true\n"
                                                     "  docker.no-new-privileges.enabled=true",
                                                 "[docker]\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.privileged-containers.enabled=false\n"
                                                     "  docker.no-new-privileges.enabled=false",
                                                 "[docker]\n"
                                                     "  docker.trusted.registries=hadoop\n"
                                                     "  docker.privileged-containers.enabled=true\n"
                                                     "  docker.no-new-privileges.enabled=false"};
    for (int i = 0; i < 2; ++i) {
      write_file(container_executor_cfg_file, container_executor_contents[i]);
      int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = create_ce_file();
      if (ret != 0) {
        std::cerr << "Could not create ce file, skipping test" << std::endl;
        return;
      }

      std::vector<std::pair<std::string, std::string> > file_cmd_vec;
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n name=container_e1_12312_11111_02_000001\n"
          "image=hadoop/docker-image\n  user=nobody",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody --security-opt=no-new-privileges "
          "--cap-drop=ALL hadoop/docker-image"));

      std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
      run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
      free_configuration(&container_executor_cfg);
    }

    for (int i = 2; i < 3; ++i) {
      write_file(container_executor_cfg_file, container_executor_contents[i]);
      int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = create_ce_file();
      if (ret != 0) {
        std::cerr << "Could not create ce file, skipping test" << std::endl;
        return;
      }

      std::vector<std::pair<std::string, std::string> > file_cmd_vec;
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n privileged=true\n  use-entry-point=true\n"
          "name=container_e1_12312_11111_02_000001\n  image=hadoop/docker-image\n  user=root",
          "run --name=container_e1_12312_11111_02_000001 --privileged --cap-drop=ALL hadoop/docker-image"));

      std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
      run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
      free_configuration(&container_executor_cfg);
    }

    for (int i = 3; i < 5; ++i) {
      write_file(container_executor_cfg_file, container_executor_contents[i]);
      int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = create_ce_file();
      if (ret != 0) {
        std::cerr << "Could not create ce file, skipping test" << std::endl;
        return;
      }

      std::vector<std::pair<std::string, std::string> > file_cmd_vec;
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n name=container_e1_12312_11111_02_000001\n"
          "image=hadoop/docker-image\n  user=nobody",
          "run --name=container_e1_12312_11111_02_000001 --user=nobody --cap-drop=ALL hadoop/docker-image"));

      std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
      run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
      free_configuration(&container_executor_cfg);
    }
  }

  TEST_F(TestDockerUtil, test_docker_exec) {
    std::string container_executor_contents = "[docker]\n"
        "  docker.allowed.devices=/dev/test\n  docker.trusted.registries=hadoop\n";
    write_file(container_executor_cfg_file, container_executor_contents);
    int ret = read_config(container_executor_cfg_file.c_str(), &container_executor_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }

    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=exec\n  name=container_e1_12312_11111_02_000001\n  launch-command=bash",
        "exec -it container_e1_12312_11111_02_000001 bash"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;

    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=exec\n  image=hadoop/docker-image\n  user=nobody",
        static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_exec_command);
    free_configuration(&container_executor_cfg);
  }

  TEST_F(TestDockerUtil, test_trusted_top_level_image) {
    struct configuration container_cfg, cmd_cfg;
    std::string container_executor_contents = "[docker]\n"
        "  docker.trusted.registries=library\n";
    write_file(container_executor_cfg_file, container_executor_contents);
    int ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    ret = create_ce_file();
    if (ret != 0) {
      std::cerr << "Could not create ce file, skipping test" << std::endl;
      return;
    }
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  image=centos",
        "centos"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  image=ubuntu:latest",
        "centos"));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  image=library/centos",
        "centos"));
    std::vector<std::pair<std::string, std::string> >::const_iterator itr;

    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = check_trusted_image(&cmd_cfg, &container_cfg);
      ASSERT_EQ(0, ret);
    }
    free_configuration(&container_cfg);
  }

  TEST_F(TestDockerUtil, test_docker_images) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=images",
        "images --format={{json .}} --filter=dangling=false"));

    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=images\n  image=image-id",
        "images image-id --format={{json .}} --filter=dangling=false"));

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=image-id",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "docker-command=images\n  image=image-id",
        static_cast<int>(INCORRECT_COMMAND)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec,
      get_docker_images_command);
    free_configuration(&container_executor_cfg);
  }

}
