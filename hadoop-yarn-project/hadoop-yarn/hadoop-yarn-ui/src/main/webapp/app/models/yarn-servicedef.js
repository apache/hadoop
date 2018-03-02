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

import DS from 'ember-data';
import Ember from 'ember';

export default DS.Model.extend({
  name: DS.attr('string', {defaultValue: ''}),
  queue: DS.attr('string', {defaultValue: ''}),
  lifetime: DS.attr('string', {defaultValue: ''}),
  isCached: DS.attr('boolean', {defaultValue: false}),

  serviceComponents: DS.attr({defaultValue: function() {
    return Ember.A();
  }}),

  serviceConfigs: DS.attr({defaultValue: function() {
    return Ember.A();
  }}),

  fileConfigs: DS.attr({defaultValue: function() {
    return Ember.A();
  }}),

  quicklinks: DS.attr({defaultValue: function() {
    return {};
  }}),

  clear() {
    this.set('name', '');
    this.set('queue', '');
    this.set('lifetime', '');
    this.get('serviceComponents').clear();
    this.get('serviceConfigs').clear();
    this.get('fileConfigs').clear();
    this.set('quicklinks', {});
  },

  isValidServiceDef() {
    return this.get('name') !== '' && this.get('queue') !== '' &&  this.get('serviceComponents.length') > 0;
  },

  createNewServiceComponent() {
    return Ember.Object.create({
      name: '',
      numOfContainers: '',
      cpus: '',
      memory: '',
      artifactId: '',
      artifactType: 'DOCKER',
      launchCommand: '',
      dependencies: [],
      uniqueComponentSupport: false,
      configuration: null
    });
  },

  createNewServiceConfig(name, value) {
    var Config = Ember.Object.extend({
      name: name || '',
      value: value || '',
      type: 'property', // property OR env OR quicklink
      scope: 'service', // service OR component
      componentName: '',
      capitalizedType: Ember.computed('type', function() {
        return Ember.String.capitalize(this.get('type'));
      }),
      formattedScope: Ember.computed('scope', 'componentName', function() {
        if (this.get('scope') !== 'service') {
          return this.get('componentName') + ' [Component]';
        }
        return Ember.String.capitalize(this.get('scope'));
      })
    });
    return Config.create();
  },

  createNewFileConfig(src, dest) {
    var FileConfig = Ember.Object.extend({
      type: 'TEMPLATE', // HADOOP_XML OR TEMPLATE
      srcFile: src || '',
      destFile: dest || '',
      scope: 'service', // service OR component
      componentName: '',
      props: null,
      formattedScope: Ember.computed('scope', 'componentName', function() {
        if (this.get('scope') !== 'service') {
          return this.get('componentName') + ' [Component]';
        }
        return Ember.String.capitalize(this.get('scope'));
      })
    });
    return FileConfig.create();
  },

  getServiceJSON() {
    return this.serializeServiceDef();
  },

  serializeServiceDef() {
    var json = {
      name: "",
      queue: "",
      lifetime: "-1",
      components: [],
      configuration: {
        properties: {},
        env: {},
        files: []
      },
      quicklinks: {}
    };

    var components = this.get('serviceComponents');
    var configs = this.get('serviceConfigs');
    var fileConfigs = this.get('fileConfigs');

    json['name'] = this.get('name');
    json['queue'] = this.get('queue');

    if (this.get('lifetime')) {
      json['lifetime'] = this.get('lifetime');
    }

    components.forEach(function(component) {
      json.components.push(this.serializeComponent(component));
    }.bind(this));

    configs.forEach(function(config) {
      let conf = this.serializeConfiguration(config);
      if (conf.scope === "service") {
        if (conf.type === "property") {
          json.configuration.properties[conf.name] = conf.value;
        } else if (conf.type === "env") {
          json.configuration.env[conf.name] = conf.value;
        } else if (conf.type === "quicklink") {
          json.quicklinks[conf.name] = conf.value;
        }
      } else if (conf.scope === "component") {
        let requiredCmp = json.components.findBy('name', conf.componentName);
        if (requiredCmp) {
          requiredCmp.configuration = requiredCmp.configuration || {};
          requiredCmp.configuration.properties = requiredCmp.configuration.properties || {};
          requiredCmp.configuration.env = requiredCmp.configuration.env || {};
          if (conf.type === "property") {
            requiredCmp.configuration.properties[conf.name] = conf.value;
          } else if (conf.type === "env") {
            requiredCmp.configuration.env[conf.name] = conf.value;
          }
        }
      }
    }.bind(this));

    fileConfigs.forEach(function(file) {
      let scope = file.get('scope');
      if (scope === "service") {
        json.configuration.files.push(this.serializeFileConfig(file));
      } else if (scope === "component") {
        let requiredCmp = json.components.findBy('name', file.get('componentName'));
        if (requiredCmp) {
          requiredCmp.configuration = requiredCmp.configuration || {};
          requiredCmp.configuration.files = requiredCmp.configuration.files || [];
          requiredCmp.configuration.files.push(this.serializeFileConfig(file));
        }
      }
    }.bind(this));

    return json;
  },

  serializeComponent(record) {
    var json = {};
    json['name'] = record.get('name');
    json['number_of_containers'] = record.get('numOfContainers');
    json['launch_command'] = record.get('launchCommand');
    json['dependencies'] = [];
    if (!Ember.isEmpty(record.get('artifactId'))) {
      json['artifact'] = {
        id: record.get('artifactId'),
        type: record.get('artifactType')
      };
    }
    json['resource'] = {
      cpus: record.get('cpus'),
      memory: record.get('memory')
    };
    if (record.get('uniqueComponentSupport')) {
      json['unique_component_support'] = "true";
    }
    if (record.get('configuration')) {
      json['configuration'] = record.get('configuration');
    }
    return json;
  },

  serializeConfiguration(config) {
    var json = {};
    json["type"] = config.get('type');
    json["scope"] = config.get('scope');
    json["componentName"] = config.get('componentName');
    json["name"] = config.get('name');
    json["value"] = config.get('value');
    return json;
  },

  serializeFileConfig(file) {
    var json = {};
    json["type"] = file.get('type');
    json["dest_file"] = file.get('destFile');
    json["src_file"] = file.get('srcFile');
    if (file.get('type') === "HADOOP_XML" && file.get('props')) {
      json["props"] = file.get('props');
    }
    return json;
  },

  createNewServiceDef() {
    return this.get('store').createRecord('yarn-servicedef', {
      id: 'yarn_servicedef_' + Date.now()
    });
  },

  convertJsonServiceConfigs(json) {
    var parsedJson = JSON.parse(json);
    if (parsedJson.properties) {
      for (let prop in parsedJson.properties) {
        if (parsedJson.properties.hasOwnProperty(prop)) {
          let newPropObj = this.createNewServiceConfig(prop, parsedJson.properties[prop]);
          this.get('serviceConfigs').addObject(newPropObj);
        }
      }
    }
    if (parsedJson.env) {
      for (let envprop in parsedJson.env) {
        if (parsedJson.env.hasOwnProperty(envprop)) {
          let newEnvObj = this.createNewServiceConfig(envprop, parsedJson.env[envprop]);
          newEnvObj.set('type', 'env');
          this.get('serviceConfigs').addObject(newEnvObj);
        }
      }
    }
  },

  convertJsonFileConfigs(json) {
    var parsedJson = JSON.parse(json);
    if (parsedJson.files) {
      parsedJson.files.forEach(function(file) {
        let newFileObj = this.createNewFileConfig(file.src_file, file.dest_file);
        this.get('fileConfigs').addObject(newFileObj);
      }.bind(this));
    }
  },

  cloneServiceDef() {
    var clone = this.createNewServiceDef();
    clone.set('name', this.get('name'));
    clone.set('queue', this.get('queue'));
    clone.set('lifetime', this.get('lifetime'));
    clone.get('serviceComponents', this.get('serviceComponents'));
    clone.get('serviceConfigs', this.get('serviceConfigs'));
    clone.get('fileConfigs', this.get('fileConfigs'));
    clone.set('quicklinks', this.get('quicklinks'));
    return clone;
  }
});
