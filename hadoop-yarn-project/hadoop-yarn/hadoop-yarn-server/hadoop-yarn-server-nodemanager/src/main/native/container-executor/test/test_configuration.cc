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

extern "C" {
#include "util.h"
#include "configuration.h"
#include "configuration.c"
}


namespace ContainerExecutor {
  class TestConfiguration : public ::testing::Test {
  protected:
    virtual void SetUp() {
      new_config_format_file = "test-configurations/configuration-1.cfg";
      old_config_format_file = "test-configurations/old-config.cfg";
      mixed_config_format_file = "test-configurations/configuration-2.cfg";
      loadConfigurations();
      return;
    }

    void loadConfigurations() {
      int ret = 0;
      ret = read_config(new_config_format_file.c_str(), &new_config_format);
      ASSERT_EQ(0, ret);
      ret = read_config(old_config_format_file.c_str(), &old_config_format);
      ASSERT_EQ(0, ret);
      ret = read_config(mixed_config_format_file.c_str(),
                        &mixed_config_format);
      ASSERT_EQ(0, ret);
    }

    virtual void TearDown() {
      free_configuration(&new_config_format);
      free_configuration(&old_config_format);
      free_configuration(&mixed_config_format);
      return;
    }

    std::string new_config_format_file;
    std::string old_config_format_file;
    std::string mixed_config_format_file;
    struct configuration new_config_format;
    struct configuration old_config_format;
    struct configuration mixed_config_format;
  };


  TEST_F(TestConfiguration, test_get_configuration_values_delimiter) {
    char **split_values;
    split_values = get_configuration_values_delimiter(NULL, "", &old_config_format, "%");
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values_delimiter("yarn.local.dirs", NULL,
                      &old_config_format, "%");
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values_delimiter("yarn.local.dirs", "",
                      NULL, "%");
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values_delimiter("yarn.local.dirs", "",
                      &old_config_format, NULL);
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values_delimiter("yarn.local.dirs", "abcd",
                                                      &old_config_format, "%");
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values_delimiter("yarn.local.dirs", "",
                      &old_config_format, "%");
    ASSERT_STREQ("/var/run/yarn", split_values[0]);
    ASSERT_STREQ("/tmp/mydir", split_values[1]);
    ASSERT_EQ(NULL, split_values[2]);
    free_values(split_values);
    split_values = get_configuration_values_delimiter("allowed.system.users",
                      "", &old_config_format, "%");
    ASSERT_STREQ("nobody,daemon", split_values[0]);
    ASSERT_EQ(NULL, split_values[1]);
    free_values(split_values);
  }

  TEST_F(TestConfiguration, test_get_configuration_values) {
    char **split_values;
    split_values = get_configuration_values(NULL, "", &old_config_format);
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values("yarn.local.dirs", NULL, &old_config_format);
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values("yarn.local.dirs", "", NULL);
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values("yarn.local.dirs", "abcd", &old_config_format);
    ASSERT_EQ(NULL, split_values);
    split_values = get_configuration_values("yarn.local.dirs", "", &old_config_format);
    ASSERT_STREQ("/var/run/yarn%/tmp/mydir", split_values[0]);
    ASSERT_EQ(NULL, split_values[1]);
    free_values(split_values);
    split_values = get_configuration_values("allowed.system.users", "",
                      &old_config_format);
    ASSERT_STREQ("nobody", split_values[0]);
    ASSERT_STREQ("daemon", split_values[1]);
    ASSERT_EQ(NULL, split_values[2]);
    free_values(split_values);
  }

  TEST_F(TestConfiguration, test_get_configuration_value) {
    std::string key_value_array[5][2] = {
        {"yarn.nodemanager.linux-container-executor.group", "yarn"},
        {"min.user.id", "1000"},
        {"allowed.system.users", "nobody,daemon"},
        {"feature.docker.enabled", "1"},
        {"yarn.local.dirs", "/var/run/yarn%/tmp/mydir"}
    };
    char *value;
    value = get_configuration_value(NULL, "", &old_config_format);
    ASSERT_EQ(NULL, value);
    value = get_configuration_value("yarn.local.dirs", NULL, &old_config_format);
    ASSERT_EQ(NULL, value);
    value = get_configuration_value("yarn.local.dirs", "", NULL);
    ASSERT_EQ(NULL, value);

    for (int i = 0; i < 5; ++i) {
      value = get_configuration_value(key_value_array[i][0].c_str(),
                "", &old_config_format);
      ASSERT_STREQ(key_value_array[i][1].c_str(), value);
      free(value);
    }
    value = get_configuration_value("test.key", "", &old_config_format);
    ASSERT_EQ(NULL, value);
    value = get_configuration_value("test.key2", "", &old_config_format);
    ASSERT_EQ(NULL, value);
    value = get_configuration_value("feature.tc.enabled", "abcd", &old_config_format);
    ASSERT_EQ(NULL, value);
  }

  TEST_F(TestConfiguration, test_no_sections_format) {
    const struct section *executor_cfg = get_configuration_section("", &old_config_format);
    char *value = NULL;
    value = get_section_value("yarn.nodemanager.linux-container-executor.group", executor_cfg);
    ASSERT_STREQ("yarn", value);
    free(value);
    value = get_section_value("feature.docker.enabled", executor_cfg);
    ASSERT_STREQ("1", value);
    free(value);
    value = get_section_value("feature.tc.enabled", executor_cfg);
    ASSERT_STREQ("0", value);
    free(value);
    value = get_section_value("min.user.id", executor_cfg);
    ASSERT_STREQ("1000", value);
    free(value);
    value = get_section_value("docker.binary", executor_cfg);
    ASSERT_STREQ("/usr/bin/docker", value);
    free(value);
    char **list = get_section_values("allowed.system.users", executor_cfg);
    ASSERT_STREQ("nobody", list[0]);
    ASSERT_STREQ("daemon", list[1]);
    free_values(list);
    list = get_section_values("banned.users", executor_cfg);
    ASSERT_STREQ("root", list[0]);
    ASSERT_STREQ("testuser1", list[1]);
    ASSERT_STREQ("testuser2", list[2]);
    free_values(list);
  }

  TEST_F(TestConfiguration, test_get_section_values_delimiter) {
    const struct section *section;
    char *value;
    char **split_values;
    section = get_configuration_section("section-1", &new_config_format);
    value = get_section_value("key1", section);
    ASSERT_STREQ("value1", value);
    free(value);
    value = get_section_value("key2", section);
    ASSERT_EQ(NULL, value);
    free(value);
    split_values = get_section_values_delimiter(NULL, section, "%");
    ASSERT_EQ(NULL, split_values);
    free_values(split_values);
    split_values = get_section_values_delimiter("split-key", NULL, "%");
    ASSERT_EQ(NULL, split_values);
    free_values(split_values);
    split_values = get_section_values_delimiter("split-key", section, NULL);
    ASSERT_EQ(NULL, split_values);
    free_values(split_values);
    split_values = get_section_values_delimiter("split-key", section, "%");
    ASSERT_FALSE(split_values == NULL);
    ASSERT_STREQ("val1,val2,val3", split_values[0]);
    ASSERT_TRUE(split_values[1] == NULL);
    free_values(split_values);
    split_values = get_section_values_delimiter("perc-key", section, "%");
    ASSERT_FALSE(split_values == NULL);
    ASSERT_STREQ("perc-val1", split_values[0]);
    ASSERT_STREQ("perc-val2", split_values[1]);
    ASSERT_TRUE(split_values[2] == NULL);
    free_values(split_values);
  }

  TEST_F(TestConfiguration, test_get_section_values) {
    const struct section *section;
    char *value;
    char **split_values;
    section = get_configuration_section("section-1", &new_config_format);
    value = get_section_value(NULL, section);
    ASSERT_EQ(NULL, value);
    free(value);
    value = get_section_value("key1", NULL);
    ASSERT_EQ(NULL, value);
    free(value);
    value = get_section_value("key1", section);
    ASSERT_STREQ("value1", value);
    free(value);
    value = get_section_value("key2", section);
    ASSERT_EQ(NULL, value);
    free(value);
    split_values = get_section_values("split-key", section);
    ASSERT_FALSE(split_values == NULL);
    ASSERT_STREQ("val1", split_values[0]);
    ASSERT_STREQ("val2", split_values[1]);
    ASSERT_STREQ("val3", split_values[2]);
    ASSERT_TRUE(split_values[3] == NULL);
    free_values(split_values);
    split_values = get_section_values("perc-key", section);
    ASSERT_FALSE(split_values == NULL);
    ASSERT_STREQ("perc-val1%perc-val2", split_values[0]);
    ASSERT_TRUE(split_values[1] == NULL);
    free_values(split_values);
    section = get_configuration_section("section-2", &new_config_format);
    value = get_section_value("key1", section);
    ASSERT_STREQ("value2", value);
    free(value);
    value = get_section_value("key2", section);
    ASSERT_STREQ("value2", value);
    free(value);
  }

  TEST_F(TestConfiguration, test_split_section) {
    const struct section *section;
    char *value;
    section = get_configuration_section("split-section", &new_config_format);
    value = get_section_value(NULL, section);
    ASSERT_EQ(NULL, value);
    free(value);
    value = get_section_value("key3", NULL);
    ASSERT_EQ(NULL, value);
    free(value);
    value = get_section_value("key3", section);
    ASSERT_STREQ("value3", value);
    free(value);
    value = get_section_value("key4", section);
    ASSERT_STREQ("value4", value);
    free(value);
  }

  TEST_F(TestConfiguration, test_get_configuration_section) {
    const struct section *section;
    ASSERT_EQ(3, new_config_format.size);
    section = get_configuration_section(NULL, &new_config_format);
    ASSERT_EQ(NULL, section);
    section = get_configuration_section("section-1", NULL);
    ASSERT_EQ(NULL, section);
    section = get_configuration_section("section-1", &new_config_format);
    ASSERT_FALSE(section == NULL);
    ASSERT_STREQ("section-1", section->name);
    ASSERT_EQ(3, section->size);
    ASSERT_FALSE(NULL == section->kv_pairs);
    section = get_configuration_section("section-2", &new_config_format);
    ASSERT_FALSE(section == NULL);
    ASSERT_STREQ("section-2", section->name);
    ASSERT_EQ(2, section->size);
    ASSERT_FALSE(NULL == section->kv_pairs);
    section = get_configuration_section("section-3", &new_config_format);
    ASSERT_TRUE(section == NULL);
  }

  TEST_F(TestConfiguration, test_read_config) {
    struct configuration config;
    int ret = 0;

    ret = read_config(NULL, &config);
    ASSERT_EQ(INVALID_CONFIG_FILE, ret);
    ret = read_config("bad-config-file", &config);
    ASSERT_EQ(INVALID_CONFIG_FILE, ret);
    ret = read_config(new_config_format_file.c_str(), &config);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(3, config.size);
    ASSERT_STREQ("section-1", config.sections[0]->name);
    ASSERT_STREQ("split-section", config.sections[1]->name);
    ASSERT_STREQ("section-2", config.sections[2]->name);
    free_configuration(&config);
    ret = read_config(old_config_format_file.c_str(), &config);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, config.size);
    ASSERT_STREQ("", config.sections[0]->name);
    free_configuration(&config);
  }

  TEST_F(TestConfiguration, test_get_kv_key) {
    int ret = 0;
    char buff[1024];
    ret = get_kv_key(NULL, buff, 1024);
    ASSERT_EQ(-EINVAL, ret);
    ret = get_kv_key("key1234", buff, 1024);
    ASSERT_EQ(-EINVAL, ret);
    ret = get_kv_key("key=abcd", NULL, 1024);
    ASSERT_EQ(-ENAMETOOLONG, ret);
    ret = get_kv_key("key=abcd", buff, 1);
    ASSERT_EQ(-ENAMETOOLONG, ret);
    ret = get_kv_key("key=abcd", buff, 1024);
    ASSERT_EQ(0, ret);
    ASSERT_STREQ("key", buff);
  }

  TEST_F(TestConfiguration, test_get_kv_value) {
    int ret = 0;
    char buff[1024];
    ret = get_kv_value(NULL, buff, 1024);
    ASSERT_EQ(-EINVAL, ret);
    ret = get_kv_value("key1234", buff, 1024);
    ASSERT_EQ(-EINVAL, ret);
    ret = get_kv_value("key=abcd", NULL, 1024);
    ASSERT_EQ(-ENAMETOOLONG, ret);
    ret = get_kv_value("key=abcd", buff, 1);
    ASSERT_EQ(-ENAMETOOLONG, ret);
    ret = get_kv_value("key=abcd", buff, 1024);
    ASSERT_EQ(0, ret);
    ASSERT_STREQ("abcd", buff);
  }

  TEST_F(TestConfiguration, test_single_section_high_key_count) {
    std::string section_name = "section-1";
    std::string sample_file_name = "large-section.cfg";
    std::ofstream sample_file;
    sample_file.open(sample_file_name.c_str());
    sample_file << "[" << section_name << "]" << std::endl;
    for(int i = 0; i < MAX_SIZE + 2; ++i) {
      sample_file << "key" << i << "=" << "value" << i << std::endl;
    }
    struct configuration cfg;
    int ret = read_config(sample_file_name.c_str(), &cfg);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, cfg.size);
    const struct section *section1 = get_configuration_section(section_name.c_str(), &cfg);
    ASSERT_EQ(MAX_SIZE + 2, section1->size);
    ASSERT_STREQ(section_name.c_str(), section1->name);
    for(int i = 0; i < MAX_SIZE + 2; ++i) {
      std::ostringstream oss;
      oss << "key" << i;
      const char *value = get_section_value(oss.str().c_str(), section1);
      oss.str("");
      oss << "value" << i;
      ASSERT_STREQ(oss.str().c_str(), value);
      free((void *) value);
    }
    remove(sample_file_name.c_str());
    free_configuration(&cfg);
  }

  TEST_F(TestConfiguration, test_multiple_sections) {
    std::string sample_file_name = "multiple-sections.cfg";
    std::ofstream sample_file;
    sample_file.open(sample_file_name.c_str());
    for(int i = 0; i < MAX_SIZE + 2; ++i) {
      sample_file << "[section-" << i << "]" << std::endl;
      sample_file << "key" << i << "=" << "value" << i << std::endl;
    }
    struct configuration cfg;
    int ret = read_config(sample_file_name.c_str(), &cfg);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(MAX_SIZE + 2, cfg.size);
    for(int i = 0; i < MAX_SIZE + 2; ++i) {
      std::ostringstream oss;
      oss << "section-" << i;
      const struct section *section = get_configuration_section(oss.str().c_str(), &cfg);
      ASSERT_EQ(1, section->size);
      ASSERT_STREQ(oss.str().c_str(), section->name);
      oss.str("");
      oss << "key" << i;
      const char *value = get_section_value(oss.str().c_str(), section);
      oss.str("");
      oss << "value" << i;
      ASSERT_STREQ(oss.str().c_str(), value);
      free((void *) value);
    }
    remove(sample_file_name.c_str());
    free_configuration(&cfg);
  }

  TEST_F(TestConfiguration, test_section_start_line) {
    const char *section_start_line = "[abcd]";
    const char *non_section_lines[] = {
        "[abcd", "abcd]", "key=value", "#abcd"
    };
    int ret = is_section_start_line(section_start_line);
    ASSERT_EQ(1, ret);
    int length = sizeof(non_section_lines) / sizeof(*non_section_lines);
    for( int i = 0; i < length; ++i) {
      ret = is_section_start_line(non_section_lines[i]);
      ASSERT_EQ(0, ret);
    }
    ret = is_section_start_line(NULL);
    ASSERT_EQ(0, ret);
  }

  TEST_F(TestConfiguration, test_comment_line) {
    const char *comment_line = "#[abcd]";
    const char *non_comment_lines[] = {
        "[abcd", "abcd]", "key=value", "[abcd]"
    };
    int ret = is_comment_line(comment_line);
    ASSERT_EQ(1, ret);
    int length = sizeof(non_comment_lines) / sizeof(*non_comment_lines);
    for( int i = 0; i < length; ++i) {
      ret = is_comment_line(non_comment_lines[i]);
      ASSERT_EQ(0, ret);
    }
    ret = is_comment_line(NULL);
    ASSERT_EQ(0, ret);
  }

  TEST_F(TestConfiguration, test_mixed_config_format) {
    const struct section *executor_cfg =
        get_configuration_section("", &mixed_config_format);
    char *value = NULL;
    value = get_section_value("key1", executor_cfg);
    ASSERT_STREQ("value1", value);
    free(value);
    value = get_section_value("key2", executor_cfg);
    ASSERT_STREQ("value2", value);
    ASSERT_EQ(2, executor_cfg->size);
    free(value);
    executor_cfg = get_configuration_section("section-1",
                                             &mixed_config_format);
    value = get_section_value("key3", executor_cfg);
    ASSERT_STREQ("value3", value);
    free(value);
    value = get_section_value("key1", executor_cfg);
    ASSERT_STREQ("value4", value);
    ASSERT_EQ(2, executor_cfg->size);
    ASSERT_EQ(2, mixed_config_format.size);
    ASSERT_STREQ("", mixed_config_format.sections[0]->name);
    ASSERT_STREQ("section-1", mixed_config_format.sections[1]->name);
    free(value);
  }
}
