/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

/*
 * NOTE: use lowercase + "_" for the option name
 */
public class CliConstants {
  public static final String RUN = "run";
  public static final String SERVE = "serve";
  public static final String LIST = "list";
  public static final String SHOW = "show";
  public static final String NAME = "name";
  public static final String INPUT_PATH = "input_path";
  public static final String CHECKPOINT_PATH = "checkpoint_path";
  public static final String SAVED_MODEL_PATH = "saved_model_path";
  public static final String N_WORKERS = "num_workers";
  public static final String N_SERVING_TASKS = "num_serving_tasks";
  public static final String N_PS = "num_ps";
  public static final String WORKER_RES = "worker_resources";
  public static final String SERVING_RES = "serving_resources";
  public static final String PS_RES = "ps_resources";
  public static final String DOCKER_IMAGE = "docker_image";
  public static final String QUEUE = "queue";
  public static final String TENSORBOARD = "tensorboard";
  public static final String TENSORBOARD_RESOURCES = "tensorboard_resources";
  public static final String TENSORBOARD_DEFAULT_RESOURCES =
      "memory=4G,vcores=1";

  public static final String WORKER_LAUNCH_CMD = "worker_launch_cmd";
  public static final String SERVING_LAUNCH_CMD = "serving_launch_cmd";
  public static final String PS_LAUNCH_CMD = "ps_launch_cmd";
  public static final String ENV = "env";
  public static final String VERBOSE = "verbose";
  public static final String SERVING_FRAMEWORK = "serving_framework";
  public static final String STOP = "stop";
  public static final String WAIT_JOB_FINISH = "wait_job_finish";
  public static final String PS_DOCKER_IMAGE = "ps_docker_image";
  public static final String WORKER_DOCKER_IMAGE = "worker_docker_image";
  public static final String QUICKLINK = "quicklink";
  public static final String TENSORBOARD_DOCKER_IMAGE =
      "tensorboard_docker_image";
}
