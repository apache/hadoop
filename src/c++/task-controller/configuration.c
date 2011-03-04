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

#include "configuration.h"


char * hadoop_conf_dir;

struct configuration config={.size=0, .confdetails=NULL};

//clean up method for freeing configuration
void free_configurations() {
  int i = 0;
  for (i = 0; i < config.size; i++) {
    if (config.confdetails[i]->key != NULL) {
      free((void *)config.confdetails[i]->key);
    }
    if (config.confdetails[i]->value != NULL) {
      free((void *)config.confdetails[i]->value);
    }
    free(config.confdetails[i]);
  }
  if (config.size > 0) {
    free(config.confdetails);
  }
  config.size = 0;
}

//function used to load the configurations present in the secure config
void get_configs() {
  FILE *conf_file;
  char *line;
  char *equaltok;
  char *temp_equaltok;
  size_t linesize = 1000;
  int size_read = 0;
  int str_len = 0;
  char *file_name = NULL;

#ifndef HADOOP_CONF_DIR
  str_len = strlen(CONF_FILE_PATTERN) + strlen(hadoop_conf_dir);
  file_name = (char *) malloc(sizeof(char) * (str_len + 1));
#else
  str_len = strlen(CONF_FILE_PATTERN) + strlen(HADOOP_CONF_DIR);
  file_name = (char *) malloc(sizeof(char) * (str_len + 1));
#endif

  if (file_name == NULL) {
    fprintf(LOGFILE, "Malloc failed :Out of memory \n");
    return;
  }
  memset(file_name,'\0',str_len +1);
#ifndef HADOOP_CONF_DIR
  snprintf(file_name,str_len, CONF_FILE_PATTERN, hadoop_conf_dir);
#else
  snprintf(file_name, str_len, CONF_FILE_PATTERN, HADOOP_CONF_DIR);
#endif

#ifdef DEBUG
  fprintf(LOGFILE,"get_configs :Conf file name is : %s \n", file_name);
#endif
  //allocate space for ten configuration items.
  config.confdetails = (struct confentry **) malloc(sizeof(struct confentry *)
      * MAX_SIZE);
  config.size = 0;
  conf_file = fopen(file_name, "r");
  if (conf_file == NULL) {
    fprintf(LOGFILE, "Invalid conf file provided : %s \n", file_name);
    free(file_name);
    return;
  }
  while(!feof(conf_file)) {
    line = (char *) malloc(linesize);
    if(line == NULL) {
      fprintf(LOGFILE,"malloc failed while reading configuration file.\n");
      goto cleanup;
    }
    size_read = getline(&line,&linesize,conf_file);
    //feof returns true only after we read past EOF.
    //so a file with no new line, at last can reach this place
    //if size_read returns negative check for eof condition
    if (size_read == -1) {
      if(!feof(conf_file)){
        fprintf(LOGFILE, "getline returned error.\n");
        goto cleanup;
      }else {
        break;
      }
    }
    //trim the ending new line
    line[strlen(line)-1] = '\0';
    //comment line
    if(line[0] == '#') {
      free(line);
      continue;
    }
    //tokenize first to get key and list of values.
    //if no equals is found ignore this line, can be an empty line also
    equaltok = strtok_r(line, "=", &temp_equaltok);
    if(equaltok == NULL) {
      free(line);
      continue;
    }
    config.confdetails[config.size] = (struct confentry *) malloc(
            sizeof(struct confentry));
    if(config.confdetails[config.size] == NULL) {
      fprintf(LOGFILE,
          "Failed allocating memory for single configuration item\n");
      goto cleanup;
    }
#ifdef DEBUG
    fprintf(LOGFILE,"get_configs : Adding conf key : %s \n", equaltok);
#endif
    memset(config.confdetails[config.size], 0, sizeof(struct confentry));
    config.confdetails[config.size]->key = (char *) malloc(
            sizeof(char) * (strlen(equaltok)+1));
    strcpy((char *)config.confdetails[config.size]->key, equaltok);
    equaltok = strtok_r(NULL, "=", &temp_equaltok);
    if (equaltok == NULL) {
      fprintf(LOGFILE, "configuration tokenization failed \n");
      goto cleanup;
    }
    //means value is commented so don't store the key
    if(equaltok[0] == '#') {
      free(line);
      free((void *)config.confdetails[config.size]->key);
      free(config.confdetails[config.size]);
      continue;
    }
#ifdef DEBUG
    fprintf(LOGFILE,"get_configs : Adding conf value : %s \n", equaltok);
#endif
    config.confdetails[config.size]->value = (char *) malloc(
            sizeof(char) * (strlen(equaltok)+1));
    strcpy((char *)config.confdetails[config.size]->value, equaltok);
    if((config.size + 1) % MAX_SIZE  == 0) {
      config.confdetails = (struct confentry **) realloc(config.confdetails,
          sizeof(struct confentry **) * (MAX_SIZE + config.size));
      if (config.confdetails == NULL) {
        fprintf(LOGFILE,
            "Failed re-allocating memory for configuration items\n");
        goto cleanup;
      }
    }
    if(config.confdetails[config.size] )
    config.size++;
    free(line);
  }

  //close the file
  fclose(conf_file);
  //clean up allocated file name
  free(file_name);
  return;
  //free spaces alloced.
  cleanup:
  if (line != NULL) {
    free(line);
  }
  fclose(conf_file);
  free(file_name);
  free_configurations();
  return;
}

/*
 * function used to get a configuration value.
 * The function for the first time populates the configuration details into
 * array, next time onwards used the populated array.
 *
 */

const char * get_value(char* key) {
  int count;
  if (config.size == 0) {
    get_configs();
  }
  if (config.size == 0) {
    fprintf(LOGFILE, "Invalid configuration provided\n");
    return NULL;
  }
  for (count = 0; count < config.size; count++) {
    if (strcmp(config.confdetails[count]->key, key) == 0) {
      return config.confdetails[count]->value;
    }
  }
  return NULL;
}

const char ** get_values(char * key) {
  char *tempTok = NULL;
  char *tempstr = NULL;
  int size = 0;
  int toPassSize = MAX_SIZE;
  const char** toPass = (const char **) malloc(sizeof(char *) * toPassSize);

  //first allocate any array of 10
  const char * value = get_value(key);
  if(value != NULL) {
    tempTok = strtok_r((char *)value, ",", &tempstr);
    while (tempTok != NULL) {
      toPass[size++] = tempTok;
      if(size == toPassSize) {
        toPassSize += MAX_SIZE;
        toPass = (const char **) realloc(toPass,(sizeof(char *) *
                                         (MAX_SIZE * toPassSize)));
      }
      tempTok = strtok_r(NULL, ",", &tempstr);
    }
  }
  toPass[size] = NULL;
  return toPass;
}

