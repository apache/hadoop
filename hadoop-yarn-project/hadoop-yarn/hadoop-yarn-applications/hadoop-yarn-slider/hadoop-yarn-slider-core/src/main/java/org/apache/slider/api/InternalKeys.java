/*
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

package org.apache.slider.api;

/**
 * Keys for internal use, go into `internal.json` and not intended for normal
 * use except when tuning Slider AM operations
 */
public interface InternalKeys {


  /**
   * Home dir of the app: {@value}
   * If set, implies there is a home dir to use
   */
  String INTERNAL_APPLICATION_HOME = "internal.application.home";
  /**
   * Path to an image file containing the app: {@value}
   */
  String INTERNAL_APPLICATION_IMAGE_PATH = "internal.application.image.path";
  /**
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   * 
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  String INTERNAL_CONTAINER_STARTUP_DELAY = "internal.container.startup.delay";
  /**
   * internal temp directory: {@value}
   */
  String INTERNAL_AM_TMP_DIR = "internal.am.tmp.dir";
  /**
   * internal temp directory: {@value}
   */
  String INTERNAL_TMP_DIR = "internal.tmp.dir";
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_SNAPSHOT_CONF_PATH = "internal.snapshot.conf.path";
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_GENERATED_CONF_PATH = "internal.generated.conf.path";
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_PROVIDER_NAME = "internal.provider.name";
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_DATA_DIR_PATH = "internal.data.dir.path";
  /**
   * where the app def is stored
   */
  String INTERNAL_APPDEF_DIR_PATH = "internal.appdef.dir.path";
  /**
   * where addons for the app are stored
   */
  String INTERNAL_ADDONS_DIR_PATH = "internal.addons.dir.path";
  /**
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   *
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  int DEFAULT_INTERNAL_CONTAINER_STARTUP_DELAY = 5000;
  /**
   * Time in seconds before a container is considered long-lived.
   * Shortlived containers are interpreted as a problem with the role
   * and/or the host: {@value}
   */
  String INTERNAL_CONTAINER_FAILURE_SHORTLIFE =
      "internal.container.failure.shortlife";
  /**
   * Default short life threshold: {@value}
   */
  int DEFAULT_INTERNAL_CONTAINER_FAILURE_SHORTLIFE = 60;
  
  /**
   * Version of the app: {@value}
   */
  String KEYTAB_LOCATION = "internal.keytab.location";

  /**
   * Queue used to deploy the app: {@value}
   */
  String INTERNAL_QUEUE = "internal.queue";

  /**
   * Flag to indicate whether or not the chaos monkey is enabled:
   * {@value}
   */
  String CHAOS_MONKEY_ENABLED = "internal.chaos.monkey.enabled";
  boolean DEFAULT_CHAOS_MONKEY_ENABLED = false;


  /**
   * Rate
   */

  String CHAOS_MONKEY_INTERVAL = "internal.chaos.monkey.interval";
  String CHAOS_MONKEY_INTERVAL_DAYS = CHAOS_MONKEY_INTERVAL + ".days";
  String CHAOS_MONKEY_INTERVAL_HOURS = CHAOS_MONKEY_INTERVAL + ".hours";
  String CHAOS_MONKEY_INTERVAL_MINUTES = CHAOS_MONKEY_INTERVAL + ".minutes";
  String CHAOS_MONKEY_INTERVAL_SECONDS = CHAOS_MONKEY_INTERVAL + ".seconds";
  
  int DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS = 0;
  int DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS = 0;
  int DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES = 0;

  String CHAOS_MONKEY_DELAY = "internal.chaos.monkey.delay";
  String CHAOS_MONKEY_DELAY_DAYS = CHAOS_MONKEY_DELAY + ".days";
  String CHAOS_MONKEY_DELAY_HOURS = CHAOS_MONKEY_DELAY + ".hours";
  String CHAOS_MONKEY_DELAY_MINUTES = CHAOS_MONKEY_DELAY + ".minutes";
  String CHAOS_MONKEY_DELAY_SECONDS = CHAOS_MONKEY_DELAY + ".seconds";
  
  int DEFAULT_CHAOS_MONKEY_STARTUP_DELAY = 0;

  /**
   * Prefix for all chaos monkey probabilities
   */
  String CHAOS_MONKEY_PROBABILITY =
      "internal.chaos.monkey.probability";
  /**
   * Probabilies are out of 10000 ; 100==1%
   */

  /**
   * Probability of a monkey check killing the AM:  {@value}
   */
  String CHAOS_MONKEY_PROBABILITY_AM_FAILURE =
      CHAOS_MONKEY_PROBABILITY + ".amfailure";

  /**
   * Default probability of a monkey check killing the AM:  {@value}
   */
  int DEFAULT_CHAOS_MONKEY_PROBABILITY_AM_FAILURE = 0;

  /**
   * Probability of a monkey check killing the AM:  {@value}
   */
  String CHAOS_MONKEY_PROBABILITY_AM_LAUNCH_FAILURE =
      CHAOS_MONKEY_PROBABILITY + ".amlaunchfailure";

  /**
   * Probability of a monkey check killing a container:  {@value}
   */

  String CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE =
      CHAOS_MONKEY_PROBABILITY + ".containerfailure";

  /**
   * Default probability of a monkey check killing the a container:  {@value}
   */
  int DEFAULT_CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE = 0;


  /**
   * 1% of chaos
   */
  int PROBABILITY_PERCENT_1 = 100;
  
  /**
   * 100% for chaos values
   */
  int PROBABILITY_PERCENT_100 = 100 * PROBABILITY_PERCENT_1;

  /**
   * interval between checks for escalation: {@value}
   */
  String ESCALATION_CHECK_INTERVAL = "escalation.check.interval.seconds";

  /**
   * default value: {@value}
   */
  int DEFAULT_ESCALATION_CHECK_INTERVAL = 30;
}
