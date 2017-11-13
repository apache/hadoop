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

    void run_docker_command_test(const std::vector<std::pair<std::string, std::string> > &file_cmd_vec,
                                 const std::vector<std::pair<std::string, int> > &bad_file_cmd_vec,
                                 int (*docker_func)(const char *, const struct configuration *, char *, const size_t)) {
      char tmp[8192];
      std::vector<std::pair<std::string, std::string> >::const_iterator itr;
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        memset(tmp, 0, 8192);
        write_command_file(itr->first);
        int ret = (*docker_func)(docker_command_file.c_str(), &container_executor_cfg, tmp, 8192);
        ASSERT_EQ(0, ret) << "error message: " << get_docker_error_message(ret) << " for input " << itr->first;
        ASSERT_STREQ(itr->second.c_str(), tmp);
      }

      std::vector<std::pair<std::string, int> >::const_iterator itr2;
      for (itr2 = bad_file_cmd_vec.begin(); itr2 != bad_file_cmd_vec.end(); ++itr2) {
        memset(tmp, 0, 8192);
        write_command_file(itr2->first);
        int ret = (*docker_func)(docker_command_file.c_str(), &container_executor_cfg, tmp, 8192);
        ASSERT_EQ(itr2->second, ret) << " for " << itr2->first << std::endl;
        ASSERT_EQ(0, strlen(tmp));
      }
      int ret = (*docker_func)("unknown-file", &container_executor_cfg, tmp, 8192);
      ASSERT_EQ(static_cast<int>(INVALID_COMMAND_FILE), ret);
    }

    void run_docker_run_helper_function(const std::vector<std::pair<std::string, std::string> > &file_cmd_vec,
                                        int (*helper_func)(const struct configuration *, char *, const size_t)) {
      std::vector<std::pair<std::string, std::string> >::const_iterator itr;
      for(itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        struct configuration cfg;
        const int buff_len = 1024;
        char buff[buff_len];
        memset(buff, 0, buff_len);
        write_command_file(itr->first);
        int ret = read_config(docker_command_file.c_str(), &cfg);
        if(ret == 0) {
          ret = (*helper_func)(&cfg, buff, buff_len);
          ASSERT_EQ(0, ret);
          ASSERT_STREQ(itr->second.c_str(), buff);
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

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  format='{{.State.Status}}'",
        static_cast<int>(INCORRECT_COMMAND)));
    bad_file_cmd_vec.push_back(
        std::make_pair<std::string, int>("docker-command=inspect\n  format='{{.State.Status}}'",
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

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_inspect_command);
  }

  TEST_F(TestDockerUtil, test_docker_load) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=load\n  image=image-id",
        "load --i='image-id' "));

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
        "pull 'image-id' "));

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
            "rm container_e1_12312_11111_02_000001"));

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

  TEST_F(TestDockerUtil, test_detach_container) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  detach=true", "-d "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, detach_container);
  }

  TEST_F(TestDockerUtil, test_rm_container) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  rm=true", "--rm "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, rm_container_on_exit);
  }

  TEST_F(TestDockerUtil, test_set_container_workdir) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  workdir=/tmp/test", "--workdir='/tmp/test' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_container_workdir);
  }

  TEST_F(TestDockerUtil, test_set_cgroup_parent) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  cgroup-parent=/sys/fs/cgroup/yarn",
        "--cgroup-parent='/sys/fs/cgroup/yarn' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_cgroup_parent);
  }

  TEST_F(TestDockerUtil, test_set_hostname) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  hostname=ctr-id", "--hostname='ctr-id' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_hostname);
  }

  TEST_F(TestDockerUtil, test_set_group_add) {
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  group-add=1000,1001", "--group-add '1000' --group-add '1001' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));

    run_docker_run_helper_function(file_cmd_vec, set_group_add);
  }

  TEST_F(TestDockerUtil, test_set_network) {
    struct configuration container_cfg;
    const int buff_len = 1024;
    char buff[buff_len];
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.allowed.networks=sdn1,bridge";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  net=bridge", "--net='bridge' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  net=sdn1", "--net='sdn1' "));
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
      memset(buff, 0, buff_len);
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_network(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), buff);
    }
    struct configuration cmd_cfg_1;
    write_command_file("[docker-command-execution]\n  docker-command=run\n  net=sdn2");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg_1);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_network(&cmd_cfg_1, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_NETWORK, ret);
    ASSERT_EQ(0, strlen(buff));

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_network(&cmd_cfg_1, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_NETWORK, ret);
    ASSERT_EQ(0, strlen(buff));
  }

  TEST_F(TestDockerUtil, test_check_mount_permitted) {
    const char *permitted_mounts[] = {"/etc", "/usr/bin/cut", "/tmp/", NULL};
    std::vector<std::pair<std::string, int> > test_data;
    test_data.push_back(std::make_pair<std::string, int>("/etc", 1));
    test_data.push_back(std::make_pair<std::string, int>("/etc/", 1));
    test_data.push_back(std::make_pair<std::string, int>("/etc/passwd", 1));
    test_data.push_back(std::make_pair<std::string, int>("/usr/bin/cut", 1));
    test_data.push_back(std::make_pair<std::string, int>("//usr/", 0));
    test_data.push_back(std::make_pair<std::string, int>("/etc/random-file", -1));

    std::vector<std::pair<std::string, int> >::const_iterator itr;
    for (itr = test_data.begin(); itr != test_data.end(); ++itr) {
      int ret = check_mount_permitted(permitted_mounts, itr->first.c_str());
      ASSERT_EQ(itr->second, ret) << "for input " << itr->first;
    }
  }

  TEST_F(TestDockerUtil, test_normalize_mounts) {
    const int entries = 4;
    const char *permitted_mounts[] = {"/home", "/etc", "/usr/bin/cut", NULL};
    const char *expected[] = {"/home/", "/etc/", "/usr/bin/cut", NULL};
    char **ptr = static_cast<char **>(malloc(entries * sizeof(char *)));
    for (int i = 0; i < entries; ++i) {
      if (permitted_mounts[i] != NULL) {
        ptr[i] = strdup(permitted_mounts[i]);
      } else {
        ptr[i] = NULL;
      }
    }
    normalize_mounts(ptr);
    for (int i = 0; i < entries; ++i) {
      ASSERT_STREQ(expected[i], ptr[i]);
    }
  }

  TEST_F(TestDockerUtil, test_set_privileged) {
    struct configuration container_cfg, cmd_cfg;
    const int buff_len = 1024;
    char buff[buff_len];
    int ret = 0;
    std::string container_executor_cfg_contents[] = {"[docker]\n  docker.privileged-containers.enabled=1",
                                                     "[docker]\n  docker.privileged-containers.enabled=0",
                                                     "[docker]\n"};
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  privileged=true", "--privileged "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  privileged=false", ""));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", ""));
    write_container_executor_cfg(container_executor_cfg_contents[0]);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      memset(buff, 0, buff_len);
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_privileged(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), buff);
    }

    // check default case and when it's turned off
    for (int i = 1; i < 3; ++i) {
      write_container_executor_cfg(container_executor_cfg_contents[i]);
      ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
      if (ret != 0) {
        FAIL();
      }
      file_cmd_vec.clear();
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n  privileged=false", ""));
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run", ""));
      for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
        memset(buff, 0, buff_len);
        write_command_file(itr->first);
        ret = read_config(docker_command_file.c_str(), &cmd_cfg);
        if (ret != 0) {
          FAIL();
        }
        ret = set_privileged(&cmd_cfg, &container_cfg, buff, buff_len);
        ASSERT_EQ(0, ret);
        ASSERT_STREQ(itr->second.c_str(), buff);
      }
      write_command_file("[docker-command-execution]\n  docker-command=run\n  privileged=true");
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_privileged(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(PRIVILEGED_CONTAINERS_DISABLED, ret);
      ASSERT_EQ(0, strlen(buff));
    }
  }

  TEST_F(TestDockerUtil, test_set_capabilities) {
    struct configuration container_cfg, cmd_cfg;
    const int buff_len = 1024;
    char buff[buff_len];
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.allowed.capabilities=CHROOT,MKNOD";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  cap-add=CHROOT,MKNOD",
        "--cap-drop='ALL' --cap-add='CHROOT' --cap-add='MKNOD' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  cap-add=CHROOT", "--cap-drop='ALL' --cap-add='CHROOT' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run", "--cap-drop='ALL' "));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      memset(buff, 0, buff_len);
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_capabilities(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), buff);
    }
    write_command_file("[docker-command-execution]\n  docker-command=run\n  cap-add=SETGID");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_capabilities(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_CAPABILITY, ret);
    ASSERT_EQ(0, strlen(buff));

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_capabilities(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_CAPABILITY, ret);
    ASSERT_EQ(0, strlen(buff));
  }

  TEST_F(TestDockerUtil, test_set_devices) {
    struct configuration container_cfg, cmd_cfg;
    const int buff_len = 1024;
    char buff[buff_len];
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.allowed.devices=/dev/test-device,/dev/device2";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  devices=/dev/test-device:/dev/test-device",
        "--device='/dev/test-device:/dev/test-device' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  devices=/dev/device2:/dev/device2",
        "--device='/dev/device2:/dev/device2' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  "
            "devices=/dev/test-device:/dev/test-device,/dev/device2:/dev/device2",
        "--device='/dev/test-device:/dev/test-device' --device='/dev/device2:/dev/device2' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n", ""));
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    if (ret != 0) {
      FAIL();
    }
    for (itr = file_cmd_vec.begin(); itr != file_cmd_vec.end(); ++itr) {
      memset(buff, 0, buff_len);
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = set_devices(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), buff);
    }
    write_command_file("[docker-command-execution]\n  docker-command=run\n  devices=/dev/device3:/dev/device3");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_devices(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, strlen(buff));

    write_command_file("[docker-command-execution]\n  docker-command=run\n  devices=/dev/device1");
    ret = read_config(docker_command_file.c_str(), &cmd_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_devices(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, strlen(buff));

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = set_devices(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_DEVICE, ret);
    ASSERT_EQ(0, strlen(buff));
  }


  TEST_F(TestDockerUtil, test_add_rw_mounts) {
    struct configuration container_cfg, cmd_cfg;
    const int buff_len = 1024;
    char buff[buff_len];
    int ret = 0;
    std::string container_executor_cfg_contents = "[docker]\n  docker.allowed.rw-mounts=/opt,/var,/usr/bin/cut,..\n  "
                                                              "docker.allowed.ro-mounts=/etc/passwd";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/var:/var", "-v '/var:/var' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/var/:/var/", "-v '/var/:/var/' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/usr/bin/cut:/usr/bin/cut",
         "-v '/usr/bin/cut:/usr/bin/cut' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/opt:/mydisk1,/var/log/:/mydisk2",
        "-v '/opt:/mydisk1' -v '/var/log/:/mydisk2' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n", ""));
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
      memset(buff, 0, buff_len);
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_rw_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), buff);
    }

    std::vector<std::pair<std::string, int> > bad_file_cmds_vec;
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/lib:/lib",
        static_cast<int>(INVALID_DOCKER_RW_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/usr/bin/:/usr/bin",
        static_cast<int>(INVALID_DOCKER_RW_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  rw-mounts=/blah:/blah",
        static_cast<int>(INVALID_DOCKER_MOUNT)));

    std::vector<std::pair<std::string, int> >::const_iterator itr2;

    for (itr2 = bad_file_cmds_vec.begin(); itr2 != bad_file_cmds_vec.end(); ++itr2) {
      memset(buff, 0, buff_len);
      write_command_file(itr2->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      strcpy(buff, "test string");
      ret = add_rw_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(itr2->second, ret);
      ASSERT_STREQ("", buff);
    }

    // verify that you can't mount any directory in the container-executor.cfg path
    char *ce_path = realpath("../etc/hadoop/container-executor.cfg", NULL);
    while (strlen(ce_path) != 0) {
      std::string cmd_file_contents = "[docker-command-execution]\n  docker-command=run\n  rw-mounts=";
      cmd_file_contents.append(ce_path).append(":").append("/etc/hadoop");
      memset(buff, 0, buff_len);
      write_command_file(cmd_file_contents);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      strcpy(buff, "test string");
      ret = add_rw_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(INVALID_DOCKER_RW_MOUNT, ret) << " for input " << cmd_file_contents;
      ASSERT_STREQ("", buff);
      char *tmp = strrchr(ce_path, '/');
      if (tmp != NULL) {
        *tmp = '\0';
      }
    }
    free(ce_path);

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    strcpy(buff, "test string");
    ret = add_rw_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_RW_MOUNT, ret);
    ASSERT_EQ(0, strlen(buff));
  }

  TEST_F(TestDockerUtil, test_add_ro_mounts) {
    struct configuration container_cfg, cmd_cfg;
    const int buff_len = 1024;
    char buff[buff_len];
    int ret = 0;

    std::string container_executor_cfg_contents = "[docker]\n  docker.allowed.rw-mounts=/home/,/var,/usr/bin/cut,..\n  "
                                                              "docker.allowed.ro-mounts=/etc/passwd,/etc/group";
    std::vector<std::pair<std::string, std::string> > file_cmd_vec;
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/var:/var", "-v '/var:/var:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/var/:/var/", "-v '/var/:/var/:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/home:/home", "-v '/home:/home:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/home/:/home", "-v '/home/:/home:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/usr/bin/cut:/usr/bin/cut",
        "-v '/usr/bin/cut:/usr/bin/cut:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/etc/group:/etc/group",
        "-v '/etc/group:/etc/group:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/etc/passwd:/etc/passwd",
        "-v '/etc/passwd:/etc/passwd:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/var/log:/mydisk1,/etc/passwd:/etc/passwd",
        "-v '/var/log:/mydisk1:ro' -v '/etc/passwd:/etc/passwd:ro' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n", ""));
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
      memset(buff, 0, buff_len);
      write_command_file(itr->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      ret = add_ro_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(0, ret);
      ASSERT_STREQ(itr->second.c_str(), buff);
    }

    std::vector<std::pair<std::string, int> > bad_file_cmds_vec;
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/etc:/etc",
        static_cast<int>(INVALID_DOCKER_RO_MOUNT)));
    bad_file_cmds_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  ro-mounts=/blah:/blah",
        static_cast<int>(INVALID_DOCKER_MOUNT)));

    std::vector<std::pair<std::string, int> >::const_iterator itr2;

    for (itr2 = bad_file_cmds_vec.begin(); itr2 != bad_file_cmds_vec.end(); ++itr2) {
      memset(buff, 0, buff_len);
      write_command_file(itr2->first);
      ret = read_config(docker_command_file.c_str(), &cmd_cfg);
      if (ret != 0) {
        FAIL();
      }
      strcpy(buff, "test string");
      ret = add_ro_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
      ASSERT_EQ(itr2->second, ret);
      ASSERT_STREQ("", buff);
    }

    container_executor_cfg_contents = "[docker]\n";
    write_container_executor_cfg(container_executor_cfg_contents);
    ret = read_config(container_executor_cfg_file.c_str(), &container_cfg);
    if (ret != 0) {
      FAIL();
    }
    write_command_file("[docker-command-execution]\n  docker-command=run\n  ro-mounts=/home:/home");
    strcpy(buff, "test string");
    ret = add_ro_mounts(&cmd_cfg, &container_cfg, buff, buff_len);
    ASSERT_EQ(INVALID_DOCKER_RO_MOUNT, ret);
    ASSERT_EQ(0, strlen(buff));
  }

  TEST_F(TestDockerUtil, test_docker_run_privileged) {

    std::string container_executor_contents = "[docker]\n  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
        "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n "
        "  docker.privileged-containers.enabled=1\n  docker.allowed.capabilities=CHOWN,SETUID\n"
        "  docker.allowed.devices=/dev/test";
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
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test",
        "run --name='container_e1_12312_11111_02_000001' --user='test' --cap-drop='ALL' 'docker-image' "));
    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name='container_e1_12312_11111_02_000001' --user='test' --cap-drop='ALL' 'docker-image' 'bash' 'test_script.sh' 'arg1' 'arg2' "));

    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name='container_e1_12312_11111_02_000001' --user='test' -d --rm -v '/var/log:/var/log:ro' -v '/var/lib:/lib:ro'"
            " -v '/usr/bin/cut:/usr/bin/cut:ro' -v '/tmp:/tmp' --cgroup-parent='ctr-cgroup' --cap-drop='ALL' --cap-add='CHOWN'"
            " --cap-add='SETUID' --hostname='host-id' --device='/dev/test:/dev/test' 'docker-image' 'bash' "
            "'test_script.sh' 'arg1' 'arg2' "));

    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  net=bridge\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name='container_e1_12312_11111_02_000001' --user='test' -d --rm --net='bridge' -v '/var/log:/var/log:ro' -v '/var/lib:/lib:ro'"
            " -v '/usr/bin/cut:/usr/bin/cut:ro' -v '/tmp:/tmp' --cgroup-parent='ctr-cgroup' --cap-drop='ALL' --cap-add='CHOWN' "
            "--cap-add='SETUID' --hostname='host-id' --device='/dev/test:/dev/test' 'docker-image' 'bash'"
            " 'test_script.sh' 'arg1' 'arg2' "));

    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  net=bridge\n  privileged=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name='container_e1_12312_11111_02_000001' --user='test' -d --rm --net='bridge' -v '/var/log:/var/log:ro' -v '/var/lib:/lib:ro'"
            " -v '/usr/bin/cut:/usr/bin/cut:ro' -v '/tmp:/tmp' --cgroup-parent='ctr-cgroup' --privileged --cap-drop='ALL' "
            "--cap-add='CHOWN' --cap-add='SETUID' --hostname='host-id' --device='/dev/test:/dev/test' 'docker-image' "
            "'bash' 'test_script.sh' 'arg1' 'arg2' "));


    file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  net=bridge\n  privileged=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n  group-add=1000,1001\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        "run --name='container_e1_12312_11111_02_000001' --user='test' -d --rm --net='bridge' -v '/var/log:/var/log:ro' -v '/var/lib:/lib:ro'"
            " -v '/usr/bin/cut:/usr/bin/cut:ro' -v '/tmp:/tmp' --cgroup-parent='ctr-cgroup' --privileged --cap-drop='ALL' "
            "--cap-add='CHOWN' --cap-add='SETUID' --hostname='host-id' --group-add '1000' --group-add '1001' "
            "--device='/dev/test:/dev/test' 'docker-image' 'bash' 'test_script.sh' 'arg1' 'arg2' "));


    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;

    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  image=docker-image\n  user=test",
        static_cast<int>(INVALID_DOCKER_CONTAINER_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  user=test\n",
        static_cast<int>(INVALID_DOCKER_IMAGE_NAME)));
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n",
        static_cast<int>(INVALID_DOCKER_USER_NAME)));

    // invalid rw mount
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/var/log:/var/log\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_RW_MOUNT)));

    // invalid ro mount
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/bin:/bin,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_RO_MOUNT)));

    // invalid capability
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n"
            "  cap-add=CHOWN,SETUID,SETGID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_CAPABILITY)));

    // invalid device
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/dev1:/dev/dev1\n  privileged=true\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_DEVICE)));

    // invalid network
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n"
            "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
            "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
            "  network=bridge\n  devices=/dev/test:/dev/test\n  privileged=true\n  net=host\n"
            "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
            "  launch-command=bash,test_script.sh,arg1,arg2",
        static_cast<int>(INVALID_DOCKER_NETWORK)));

    run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
  }

  TEST_F(TestDockerUtil, test_docker_run_no_privileged) {

    std::string container_executor_contents[] = {"[docker]\n  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
                                                     "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n "
                                                     "  docker.allowed.capabilities=CHOWN,SETUID\n"
                                                     "  docker.allowed.devices=/dev/test",
                                                 "[docker]\n  docker.allowed.ro-mounts=/var,/etc,/usr/bin/cut\n"
                                                     "  docker.allowed.rw-mounts=/tmp\n  docker.allowed.networks=bridge\n "
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
          "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test",
          "run --name='container_e1_12312_11111_02_000001' --user='test' --cap-drop='ALL' 'docker-image' "));
      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n"
              "  user=test\n  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name='container_e1_12312_11111_02_000001' --user='test' --cap-drop='ALL' 'docker-image' 'bash' 'test_script.sh' 'arg1' 'arg2' "));

      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
              "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
              "  network=bridge\n  devices=/dev/test:/dev/test\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name='container_e1_12312_11111_02_000001' --user='test' -d --rm -v '/var/log:/var/log:ro' -v '/var/lib:/lib:ro'"
              " -v '/usr/bin/cut:/usr/bin/cut:ro' -v '/tmp:/tmp' --cgroup-parent='ctr-cgroup' --cap-drop='ALL' --cap-add='CHOWN'"
              " --cap-add='SETUID' --hostname='host-id' --device='/dev/test:/dev/test' 'docker-image' 'bash' "
              "'test_script.sh' 'arg1' 'arg2' "));

      file_cmd_vec.push_back(std::make_pair<std::string, std::string>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
              "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
              "  network=bridge\n  devices=/dev/test:/dev/test\n  net=bridge\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          "run --name='container_e1_12312_11111_02_000001' --user='test' -d --rm --net='bridge' -v '/var/log:/var/log:ro' -v '/var/lib:/lib:ro'"
              " -v '/usr/bin/cut:/usr/bin/cut:ro' -v '/tmp:/tmp' --cgroup-parent='ctr-cgroup' --cap-drop='ALL' --cap-add='CHOWN' "
              "--cap-add='SETUID' --hostname='host-id' --device='/dev/test:/dev/test' 'docker-image' 'bash'"
              " 'test_script.sh' 'arg1' 'arg2' "));

      std::vector<std::pair<std::string, int> > bad_file_cmd_vec;
      bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
          "[docker-command-execution]\n"
              "  docker-command=run\n  name=container_e1_12312_11111_02_000001\n  image=docker-image\n  user=test\n  hostname=host-id\n"
              "  ro-mounts=/var/log:/var/log,/var/lib:/lib,/usr/bin/cut:/usr/bin/cut\n  rw-mounts=/tmp:/tmp\n"
              "  network=bridge\n  devices=/dev/test:/dev/test\n  net=bridge\n  privileged=true\n"
              "  cap-add=CHOWN,SETUID\n  cgroup-parent=ctr-cgroup\n  detach=true\n  rm=true\n"
              "  launch-command=bash,test_script.sh,arg1,arg2",
          static_cast<int>(PRIVILEGED_CONTAINERS_DISABLED)));

      run_docker_command_test(file_cmd_vec, bad_file_cmd_vec, get_docker_run_command);
    }
  }

  TEST_F(TestDockerUtil, test_docker_config_param) {
    std::vector<std::pair<std::string, std::string> > input_output_map;
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=inspect\n  docker-config=/my-config\n"
            "  format={{.State.Status}}\n  name=container_e1_12312_11111_02_000001",
        "--config='/my-config' inspect --format={{.State.Status}} container_e1_12312_11111_02_000001"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=load\n  docker-config=/my-config\n  image=image-id",
        "--config='/my-config' load --i='image-id' "));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=pull\n  docker-config=/my-config\n  image=image-id",
        "--config='/my-config' pull 'image-id' "));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=rm\n  docker-config=/my-config\n  name=container_e1_12312_11111_02_000001",
        "--config='/my-config' rm container_e1_12312_11111_02_000001"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=stop\n  docker-config=/my-config\n  name=container_e1_12312_11111_02_000001",
        "--config='/my-config' stop container_e1_12312_11111_02_000001"));
    input_output_map.push_back(std::make_pair<std::string, std::string>(
        "[docker-command-execution]\n  docker-command=run\n  docker-config=/my-config\n  name=container_e1_12312_11111_02_000001\n"
            "  image=docker-image\n  user=test",
        "--config='/my-config' run --name='container_e1_12312_11111_02_000001' --user='test' --cap-drop='ALL' 'docker-image' "));

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    char buffer[4096];
    struct configuration cfg = {0, NULL};
    for (itr = input_output_map.begin(); itr != input_output_map.end(); ++itr) {
      memset(buffer, 0, 4096);
      write_command_file(itr->first);
      int ret = get_docker_command(docker_command_file.c_str(), &cfg, buffer, 4096);
      ASSERT_EQ(0, ret) << "for input " << itr->first;
      ASSERT_STREQ(itr->second.c_str(), buffer);
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

    std::vector<std::pair<std::string, int> > bad_file_cmd_vec;

    // Wrong subcommand
    bad_file_cmd_vec.push_back(std::make_pair<std::string, int>(
        "[docker-command-execution]\n  docker-command=volume\n  sub-command=ls\n  volume=volume1 \n driver=driver1",
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
  }
}
