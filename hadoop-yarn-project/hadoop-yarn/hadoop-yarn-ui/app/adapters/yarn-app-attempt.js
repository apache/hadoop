import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';
import Config from 'yarn-ui/config';

export default DS.JSONAPIAdapter.extend({
  headers: {
    Accept: 'application/json'
  },
  host: 'http://localhost:1337/' + Config.RM_HOST + ':' + Config.RM_PORT, // configurable
  namespace: 'ws/v1/cluster', // common const

  urlForQuery(query, modelName) {
    var url = this._buildURL();
    return url + '/apps/' + query.appId + "/appattempts";
  },

  urlForFindRecord(id, modelName, snapshot) {
    var url = this._buildURL();
    var url = url + '/apps/' + 
           Converter.attemptIdToAppId(id) + "/appattempts/" + id;
    console.log(url);
    return url;
  },

  ajax(url, method, hash) {
    hash = {};
    hash.crossDomain = true;
    hash.xhrFields = {withCredentials: true};
    hash.targetServer = "RM";
    return this._super(url, method, hash); 
  }
});
