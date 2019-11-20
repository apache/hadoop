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

#include <algorithm>
#include <gtest/gtest.h>
#include <fstream>
#include "errno.h"
#include "configuration.h"
#include <pwd.h>
#include <stdbool.h>

extern "C" {
    #include "container-executor.h"
    #include "runc/runc_launch_cmd.h"
    #include "runc/runc_write_config.h"
    #include "utils/mount-utils.h"
}

#define STARTING_JSON_BUFFER_SIZE  (128*1024)

namespace ContainerExecutor {

    class TestRunc : public ::testing::Test {
    protected:
        virtual void SetUp() {
            container_executor_cfg_file = "container-executor.cfg";
            std::string container_executor_cfg_contents = "[runc]\n  "
                                                          "runc.allowed.rw-mounts=/opt,/var,/usr/bin/cut,/usr/bin/awk\n  "
                                                          "runc.allowed.ro-mounts=/etc/passwd";

            int ret = setup_container_executor_cfg(container_executor_cfg_contents);
            ASSERT_EQ(ret, 0) << "Container executor cfg setup failed\n";
        }

        virtual void TearDown() {
            remove(runc_config_file);
            remove(container_executor_cfg_file.c_str());
            delete_ce_file();
            free_executor_configurations();
        }

        const char *runc_config_file = "runc-config.json";
        std::string container_executor_cfg_file;


        void write_file(const std::string fname, const std::string contents) {
            std::ofstream config_file;
            config_file.open(fname.c_str());
            config_file << contents;
            config_file.close();
        }

        int create_ce_file() {
            int ret = 0;
            const char *fname = HADOOP_CONF_DIR "/" CONF_FILENAME;
            struct stat buffer;

            if (stat(fname, &buffer) == 0) {
                return ret;
            }

            if (strcmp("../etc/hadoop/container-executor.cfg", fname) == 0) {
                ret = mkdir("../etc", 0755);
                if (ret == 0 || errno == EEXIST) {
                    ret = mkdir("../etc/hadoop", 0755);
                    if (ret == 0 || errno == EEXIST) {
                        write_file("../etc/hadoop/container-executor.cfg", "");
                    } else {
                        std::cerr << "Could not create ../etc/hadoop, " << strerror(errno) << std::endl;
                    }
                } else {
                    std::cerr << "Could not create ../etc, " << strerror(errno) << std::endl;
                }
            } else {
                // Don't want to create directories all over. Make a simple attempt to
                // write the file.
                write_file(fname, "");
            }

            if (stat(fname, &buffer) != 0) {
              std::cerr << "Could not create " << fname << strerror(errno) << std::endl;
              ret = 1;
            }

          return ret;
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

        void write_config_file(const std::string contents) {
            write_file(runc_config_file, contents);
        }

        int setup_container_executor_cfg(const std::string container_executor_cfg_contents) {
            write_container_executor_cfg(container_executor_cfg_contents);
            read_executor_config(container_executor_cfg_file.c_str());

          return create_ce_file();
        }

        static cJSON* build_layer_array(const rlc_layer_spec *layers_array, int num_layers) {
            cJSON* layer_json_array = cJSON_CreateArray();
            if (layer_json_array == NULL) { return NULL; }

            int i;

            for (i = 0; i < num_layers; i++) {
                const rlc_layer_spec *layer = &layers_array[i];
                cJSON* layer_json = cJSON_CreateObject();
                if (layer_json == NULL) { goto fail; }
                cJSON_AddStringToObject(layer_json, "mediaType", layer->media_type);
                cJSON_AddStringToObject(layer_json, "path", layer->path);
                cJSON_AddItemToArray(layer_json_array, layer_json);
            }
            return layer_json_array;

fail:
            cJSON_Delete(layer_json_array);
            return NULL;
        }

        static cJSON* build_string_array(char const* const* dirs_array) {
            int i;
            cJSON* dirs_json_array = cJSON_CreateArray();
            if (dirs_json_array == NULL) { return NULL; }

            for (i = 0; dirs_array[i] != NULL; i++) {
                const char *dir = dirs_array[i];
                cJSON* layer_json = cJSON_CreateString(dir);
                if (layer_json == NULL) { goto fail; }
                cJSON_AddItemToArray(dirs_json_array, layer_json);
            }

            return dirs_json_array;

fail:
            cJSON_Delete(dirs_json_array);
            return NULL;
        }

        static bool build_process_struct(runc_config_process *const process, char const* const* args, char const* const cwd,
            char const* const* env) {
            cJSON *args_array = build_string_array(args);
            if (args_array == NULL) { return false; }
            cJSON *cwd_string = cJSON_CreateString(cwd);
            if (cwd_string == NULL) { return false; }
            cJSON *env_array = build_string_array(env);
            if (env_array == NULL) { return false; }

            process->args = args_array;
            process->cwd = cwd_string;
            process->env = env_array;

            return true;
        }

        static cJSON* build_process_json(runc_config_process const* const process) {
            cJSON* process_json = cJSON_CreateObject();
            if (process_json == NULL) { return NULL; }

            cJSON_AddItemReferenceToObject(process_json, "args", process->args);
            cJSON_AddItemReferenceToObject(process_json, "cwd", process->cwd);
            cJSON_AddItemReferenceToObject(process_json, "env", process->env);

            return process_json;
        }

        static cJSON* build_config_json(runc_config const* const config) {
            cJSON *config_json = cJSON_CreateObject();
            if (config_json == NULL) { return NULL; }

            runc_config_process const *const process = &config->process;
            cJSON *process_json = build_process_json(process);
            if (process_json == NULL) { goto fail; }


            cJSON_AddItemToObject(config_json, "process", process_json);
            cJSON_AddItemReferenceToObject(config_json, "linux", config->linux_config);
            cJSON_AddItemReferenceToObject(config_json, "mounts", config->mounts);

            return config_json;

fail:
            cJSON_Delete(config_json);
            return NULL;
        }

        static cJSON* build_runc_config_json(runc_launch_cmd const* const rlc) {
            cJSON *local_dir_json;
            cJSON *log_dir_json;
            cJSON *layer_dir_json;
            cJSON *runc_config_json = cJSON_CreateObject();
            if (runc_config_json == NULL) { return NULL; }

            runc_config const* const config = &rlc->config;

            cJSON* config_json = build_config_json(config);
            if (config_json == NULL) { goto fail; }

            cJSON_AddItemToObject(runc_config_json, "ociRuntimeConfig", config_json);
            cJSON_AddStringToObject(runc_config_json, "runAsUser", rlc->run_as_user);
            cJSON_AddStringToObject(runc_config_json, "username", rlc->username);
            cJSON_AddStringToObject(runc_config_json, "applicationId", rlc->app_id);
            cJSON_AddStringToObject(runc_config_json, "containerId", rlc->container_id);
            cJSON_AddStringToObject(runc_config_json, "pidFile", rlc->pid_file);
            cJSON_AddStringToObject(runc_config_json, "containerScriptPath", rlc->script_path);
            cJSON_AddStringToObject(runc_config_json, "containerCredentialsPath", rlc->cred_path);
            cJSON_AddNumberToObject(runc_config_json, "https", rlc->https);

            local_dir_json = build_string_array(rlc->local_dirs);
            log_dir_json = build_string_array(rlc->log_dirs);
            layer_dir_json = build_layer_array(rlc->layers, rlc->num_layers);

            cJSON_AddItemToObject(runc_config_json, "layers", layer_dir_json);
            cJSON_AddItemToObject(runc_config_json, "localDirs", local_dir_json);
            cJSON_AddItemToObject(runc_config_json, "logDirs", log_dir_json);

            cJSON_AddNumberToObject(runc_config_json, "reapLayerKeepCount", rlc->num_reap_layers_keep);
            return runc_config_json;

fail:
            cJSON_Delete(runc_config_json);
            return NULL;
        }

        static cJSON* build_mount_json(const char *src, const char *dest, const char *input_options[]) {
            cJSON* mount_json = cJSON_CreateObject();
            if (mount_json == NULL) { return NULL; }

            cJSON_AddStringToObject(mount_json, "source", src);
            cJSON_AddStringToObject(mount_json, "destination", dest);
            cJSON_AddStringToObject(mount_json, "type", "bind");

            cJSON* options_array = build_string_array(input_options);
            cJSON_AddItemToObject(mount_json, "options", options_array);

            return mount_json;
        }


        static cJSON* build_mounts_json() {
            cJSON* mount_json = NULL;

            cJSON* mounts_json_array = cJSON_CreateArray();
            if (mounts_json_array == NULL) {
                return NULL;
            }

            const char *options_rw[10] = {"rw", "rprivate", "rbind"};
            const char *options_ro[10] = {"ro", "rprivate", "rbind"};

            mount_json = build_mount_json("/opt", "/opt", options_rw);
            if (mount_json == NULL) { goto fail; }
            cJSON_AddItemToArray(mounts_json_array, mount_json);

            mount_json = build_mount_json("/var/", "/var/", options_rw);
            if (mount_json == NULL) { goto fail; }
            cJSON_AddItemToArray(mounts_json_array, mount_json);

            mount_json = build_mount_json("/etc/passwd", "/etc/passwd", options_ro);
            if (mount_json == NULL) { goto fail; }
            cJSON_AddItemToArray(mounts_json_array, mount_json);

            return mounts_json_array;

fail:
            cJSON_Delete(mounts_json_array);
            return NULL;

        }

        static runc_launch_cmd* build_default_runc_launch_cmd() {
            char* run_as_user = strdup(getpwuid(getuid())->pw_name);
            char* username = strdup(getpwuid(getuid())->pw_name);
            char const* const application_id = "application_1571614753172_3182915";
            char const* const container_id = "container_e14_1571614753172_3182915_01_000001";
            char const* const pid_file = "/tmp/container_e14_1571614753172_3182915_01_000001.pid";
            char const* const container_script_path = "/tmp/launch_container.sh";
            char const* const container_credentials_path = "/tmp/container_e14_1571614753172_3182915_01_000001.tokens";
            char const* const log_dirs[] = {"/log1", "/log2", NULL};
            char const* const local_dirs[] = {"/local1", "/local2", NULL};
            char const* const layer_media_type = "application/vnd.squashfs";
            char const* const args[] = {"bash", "/tmp/launch_container.sh", NULL};
            char const* const cwd = "/tmp";
            char const* const env[] = {"HADOOP_PREFIX=/tmp", "PATH=/tmp", NULL};
            char const* const hostname = "hostname";
            int num_reap_layers_keep = 10;
            unsigned int num_layers = 2;
            int https = 1;
            int i;

            runc_launch_cmd *rlc_input = NULL;
            rlc_layer_spec *layers_input = NULL;
            cJSON *hostname_json = NULL;
            cJSON *linux_config_json = NULL;
            cJSON *mounts_json = NULL;

            rlc_input = (runc_launch_cmd*) calloc(1, sizeof(*rlc_input));
            if (rlc_input == NULL) { return NULL; }

            runc_config *config = &rlc_input->config;

            rlc_input->run_as_user = run_as_user;
            rlc_input->username = username;
            rlc_input->app_id = strdup(application_id);
            rlc_input->container_id = strdup(container_id);
            rlc_input->pid_file = strdup(pid_file);
            rlc_input->script_path = strdup(container_script_path);
            rlc_input->cred_path = strdup(container_credentials_path);
            rlc_input->https = https;

            rlc_input->local_dirs = (char **) calloc(sizeof(local_dirs)/sizeof(local_dirs[0]) + 1, sizeof(*local_dirs));
            for (i = 0; local_dirs[i] != NULL; i++) {
                rlc_input->local_dirs[i] = strdup(local_dirs[i]);
            }
            rlc_input->local_dirs[i] = NULL;

            rlc_input->log_dirs = (char **) calloc(sizeof(log_dirs)/sizeof(log_dirs[0]) + 1, sizeof(*log_dirs));
            for (i = 0; log_dirs[i] != NULL; i++) {
                rlc_input->log_dirs[i] = strdup(log_dirs[i]);
            }
            rlc_input->log_dirs[i] = NULL;

            rlc_input->num_reap_layers_keep = num_reap_layers_keep;
            rlc_input->num_layers = num_layers;

            layers_input = (rlc_layer_spec*) calloc(num_layers, sizeof(*layers_input));
            (&layers_input[0])->media_type = strdup(layer_media_type);
            (&layers_input[0])->path = strdup("/foo");
            (&layers_input[1])->media_type = strdup(layer_media_type);
            (&layers_input[1])->path = strdup("/bar");

            rlc_input->layers = layers_input;

            build_process_struct(&config->process, args, cwd, env);

            hostname_json = cJSON_CreateString(hostname);
            if (hostname_json == NULL) { goto fail; }
            config->hostname = hostname_json;

            linux_config_json = cJSON_CreateObject();
            if (linux_config_json == NULL) { goto fail; }
            config->linux_config = linux_config_json;

            mounts_json = build_mounts_json();
            if (mounts_json == NULL) { goto fail; }
            config->mounts = mounts_json;

            return rlc_input;
fail:
            free_runc_launch_cmd(rlc_input);
            return NULL;
        }


        static void test_runc_launch_cmd(runc_launch_cmd const* const rlc_input, runc_launch_cmd const* const rlc_parsed) {
            unsigned int i;

            ASSERT_STREQ(rlc_parsed->run_as_user, rlc_input->run_as_user);
            ASSERT_STREQ(rlc_parsed->username, rlc_input->username);
            ASSERT_STREQ(rlc_parsed->app_id, rlc_input->app_id);
            ASSERT_STREQ(rlc_parsed->container_id, rlc_input->container_id);
            ASSERT_STREQ(rlc_parsed->pid_file, rlc_input->pid_file);
            ASSERT_STREQ(rlc_parsed->script_path, rlc_input->script_path);
            ASSERT_STREQ(rlc_parsed->cred_path, rlc_input->cred_path);
            ASSERT_EQ(rlc_parsed->https, rlc_input->https);

            for (i = 0; rlc_input->local_dirs[i] != NULL; i++) {
                ASSERT_NE(rlc_input->local_dirs[i], nullptr);
                ASSERT_NE(rlc_parsed->local_dirs[i], nullptr);
                ASSERT_STREQ(rlc_parsed->local_dirs[i], rlc_input->local_dirs[i]);
            }

            for (i = 0; rlc_input->log_dirs[i] != NULL; i++) {
                ASSERT_NE(rlc_input->log_dirs[i], nullptr);
                ASSERT_NE(rlc_parsed->log_dirs[i], nullptr);
                ASSERT_STREQ(rlc_parsed->log_dirs[i], rlc_input->log_dirs[i]);
            }

            for (i = 0; i < rlc_input->num_layers; i++) {
                rlc_layer_spec *layer_input = &rlc_input->layers[i];
                rlc_layer_spec *layer_parsed = &rlc_parsed->layers[i];
                ASSERT_NE(layer_input, nullptr);
                ASSERT_NE(layer_parsed, nullptr);
                ASSERT_STREQ(layer_parsed->media_type, layer_input->media_type);
                ASSERT_STREQ(layer_parsed->path, layer_input->path);
            }

            ASSERT_EQ(rlc_parsed->num_layers, rlc_input->num_layers);
            ASSERT_EQ(rlc_parsed->num_reap_layers_keep, rlc_input->num_reap_layers_keep);
        }
    };

    static mount* build_mounts(std::string mounts_string,
                               unsigned int num_mounts) {

        mount* mounts = (mount*) calloc(num_mounts, sizeof(*mounts));
        std::istringstream comma_iss(mounts_string);
        std::string comma_token;
        int i = 0;
        while (std::getline(comma_iss, comma_token, ','))
        {
            mount_options *options = (mount_options*) calloc(1, sizeof(*options));
            mounts[i].options = options;
            std::istringstream colon_iss(comma_token);
            std::string colon_token;

            std::getline(colon_iss, colon_token, ':');
            mounts[i].src = strdup(colon_token.c_str());

            std::getline(colon_iss, colon_token, ':');
            mounts[i].dest = strdup(colon_token.c_str());

            std::getline(colon_iss, colon_token, ':');
            std::istringstream plus_iss(colon_token);
            std::string plus_token;

            unsigned int num_opts = std::count(colon_token.begin(), colon_token.end(), '+') + 1;
            int j = 0;
            char **opts = (char**) calloc(num_opts + 1, sizeof(*opts));
            mounts[i].options->opts = opts;
            while(std::getline(plus_iss, plus_token, '+')) {
                char *mount_option = strdup(plus_token.c_str());
                if (strcmp("rw", mount_option) == 0) {
                    options->rw = 1;
                } else if (strcmp("ro", mount_option) == 0) {
                    options->rw = 0;
                }
                options->opts[j] = mount_option;
                j++;
            }
            options->opts[j] = NULL;
            options->num_opts = num_opts;
            i++;
        }
        return mounts;
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_valid) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        rlc_input = build_default_runc_launch_cmd();
        ASSERT_NE(rlc_input, nullptr);

        runc_config_json = build_runc_config_json(rlc_input);
        ASSERT_NE(runc_config_json, nullptr);

        json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
        write_config_file(json_data);

        rlc_parsed = parse_runc_launch_cmd(runc_config_file);
        ASSERT_NE(rlc_parsed, nullptr);

        ret = is_valid_runc_launch_cmd(rlc_parsed);
        ASSERT_NE(ret, false);

        test_runc_launch_cmd(rlc_input, rlc_parsed);

        cJSON_Delete(runc_config_json);
        free_runc_launch_cmd(rlc_input);
        free(json_data);
        free_runc_launch_cmd(rlc_parsed);
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_bad_container_id) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        rlc_input = build_default_runc_launch_cmd();
        ASSERT_NE(rlc_input, nullptr);

        free(rlc_input->container_id);
        rlc_input->container_id = strdup("foobar");

        runc_config_json = build_runc_config_json(rlc_input);
        ASSERT_NE(runc_config_json, nullptr);

        json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
        write_config_file(json_data);

        rlc_parsed = parse_runc_launch_cmd(runc_config_file);
        ASSERT_NE(rlc_parsed, nullptr);

        ret = is_valid_runc_launch_cmd(rlc_parsed);
        // An invalid container_id should cause an error and the parse function should return null
        ASSERT_EQ(ret, false);

        cJSON_Delete(runc_config_json);
        free_runc_launch_cmd(rlc_input);
        free(json_data);
        free_runc_launch_cmd(rlc_parsed);
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_existing_pidfile) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        rlc_input = build_default_runc_launch_cmd();
        ASSERT_NE(rlc_input, nullptr);

        free(rlc_input->pid_file);
        const char* pid_file = "/tmp/foo";
        rlc_input->pid_file = strdup(pid_file);
        write_file(pid_file, "");

        runc_config_json = build_runc_config_json(rlc_input);
        ASSERT_NE(runc_config_json, nullptr);

        json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
        write_config_file(json_data);

        rlc_parsed = parse_runc_launch_cmd(runc_config_file);
        ASSERT_NE(rlc_parsed, nullptr);

        ret = is_valid_runc_launch_cmd(rlc_parsed);
        // A pid file that already exists should cause an error and the parse function should return null
        ASSERT_EQ(ret, false);

        remove(pid_file);
        cJSON_Delete(runc_config_json);
        free_runc_launch_cmd(rlc_input);
        free(json_data);
        free_runc_launch_cmd(rlc_parsed);
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_invalid_media_type) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        rlc_input = build_default_runc_launch_cmd();
        ASSERT_NE(rlc_input, nullptr);

        free(rlc_input->layers[0].media_type);
        rlc_input->layers[0].media_type = strdup("bad media type");

        runc_config_json = build_runc_config_json(rlc_input);
        ASSERT_NE(runc_config_json, nullptr);

        json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
        write_config_file(json_data);

        rlc_parsed = parse_runc_launch_cmd(runc_config_file);
        ASSERT_NE(rlc_parsed, nullptr);

        ret = is_valid_runc_launch_cmd(rlc_parsed);
        // A bad layer media type should cause an error and the parse function should return null
        ASSERT_EQ(ret, false);

        cJSON_Delete(runc_config_json);
        free_runc_launch_cmd(rlc_input);
        free(json_data);
        free_runc_launch_cmd(rlc_parsed);
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_invalid_num_reap_layers_keep) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        rlc_input = build_default_runc_launch_cmd();
        ASSERT_NE(rlc_input, nullptr);

        rlc_input->num_reap_layers_keep = -1;

        runc_config_json = build_runc_config_json(rlc_input);
        ASSERT_NE(runc_config_json, nullptr);

        json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
        write_config_file(json_data);

        rlc_parsed = parse_runc_launch_cmd(runc_config_file);
        ASSERT_NE(rlc_parsed, nullptr);

        ret = is_valid_runc_launch_cmd(rlc_parsed);
        // A negative num_reap_layers_keep value should cause an error and the parse function should return null
        ASSERT_EQ(ret, false);

        cJSON_Delete(runc_config_json);
        free_runc_launch_cmd(rlc_input);
        free(json_data);
        free_runc_launch_cmd(rlc_parsed);
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_valid_mounts) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        std::vector<std::string> mounts_string_vec;

        mounts_string_vec.push_back(std::string("/var:/var:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/var:/var:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/var/:/var/:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/usr/bin/cut:/usr/bin/cut:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string(
            "/usr/bin/awk:/awk:rw+shared+rbind+rprivate,/etc/passwd:/etc/passwd:ro+rbind+rprivate"));
        mounts_string_vec.push_back(std::string(
            "/var:/var:ro+rprivate+rbind,/etc/passwd:/etc/passwd:ro+rshared+rbind+rprivate"));

        mount *mounts = NULL;
        std::vector<std::string>::const_iterator itr;

        for (itr = mounts_string_vec.begin(); itr != mounts_string_vec.end(); ++itr) {
            rlc_input = build_default_runc_launch_cmd();
            ASSERT_NE(rlc_input, nullptr);

            cJSON* mounts_json_array = cJSON_CreateArray();
            ASSERT_NE(mounts_json_array, nullptr);

            unsigned int num_mounts = std::count(itr->begin(), itr->end(), ',') + 1;
            mounts = build_mounts(*itr, num_mounts);
            for (unsigned int i = 0; i < num_mounts; i++) {
                cJSON *mount_json = build_mount_json(mounts[i].src,
                                                     mounts[i].dest,
                                                     (const char**) mounts[i].options->opts);
                ASSERT_NE(mount_json, nullptr);
                cJSON_AddItemToArray(mounts_json_array, mount_json);
            }

            cJSON_Delete(rlc_input->config.mounts);
            rlc_input->config.mounts = mounts_json_array;

            runc_config_json = build_runc_config_json(rlc_input);
            ASSERT_NE(runc_config_json, nullptr);

            json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
            write_config_file(json_data);

            rlc_parsed = parse_runc_launch_cmd(runc_config_file);
            ASSERT_NE(rlc_parsed, nullptr);

            ret = is_valid_runc_launch_cmd(rlc_parsed);
            ASSERT_NE(ret, false);

            test_runc_launch_cmd(rlc_input, rlc_parsed );

            cJSON_Delete(runc_config_json);
            free_mounts(mounts, num_mounts);
            free_runc_launch_cmd(rlc_input);
            free(json_data);
            free_runc_launch_cmd(rlc_parsed);
        }
    }

    TEST_F(TestRunc, test_parse_runc_launch_cmd_invalid_mounts) {
        runc_launch_cmd *rlc_input = NULL;
        runc_launch_cmd *rlc_parsed = NULL;
        cJSON *runc_config_json = NULL;
        char* json_data = NULL;
        int ret = 0;

        std::vector<std::string> mounts_string_vec;

        mounts_string_vec.push_back(std::string("/lib:/lib:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/lib:/lib:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/usr/bin/:/usr/bin:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/blah:/blah:rw+rbind+rprivate"));
        mounts_string_vec.push_back(std::string("/tmp:/tmp:shared"));
        mounts_string_vec.push_back(std::string("/lib:/lib"));
        mounts_string_vec.push_back(std::string("/lib:/lib:other"));

        mount *mounts = NULL;
        std::vector<std::string>::const_iterator itr;

        for (itr = mounts_string_vec.begin(); itr != mounts_string_vec.end(); ++itr) {
            rlc_input = build_default_runc_launch_cmd();
            ASSERT_NE(rlc_input, nullptr);

            cJSON* mounts_json_array = cJSON_CreateArray();
            ASSERT_NE(mounts_json_array, nullptr);

            unsigned int num_mounts = std::count(itr->begin(), itr->end(), ',') + 1;
            mounts = build_mounts(*itr, num_mounts);
            for (unsigned int i = 0; i < num_mounts; i++) {
                cJSON *mount_json = build_mount_json(mounts[i].src,
                                                     mounts[i].dest,
                                                     (const char**) mounts[i].options->opts);
                ASSERT_NE(mount_json, nullptr);
                cJSON_AddItemToArray(mounts_json_array, mount_json);
            }

            cJSON_Delete(rlc_input->config.mounts);
            rlc_input->config.mounts = mounts_json_array;

            runc_config_json = build_runc_config_json(rlc_input);
            ASSERT_NE(runc_config_json, nullptr);

            json_data = cJSON_PrintBuffered(runc_config_json, STARTING_JSON_BUFFER_SIZE, false);
            write_config_file(json_data);

            rlc_parsed = parse_runc_launch_cmd(runc_config_file);
            ASSERT_NE(rlc_parsed, nullptr);

            ret = is_valid_runc_launch_cmd(rlc_parsed);
            // A invalid mount should cause an error and the parse function should return null
            ASSERT_EQ(ret, false);

            cJSON_Delete(runc_config_json);
            free_mounts(mounts, num_mounts);
            free_runc_launch_cmd(rlc_input);
            free(json_data);
            free_runc_launch_cmd(rlc_parsed);
        }
    }
}

