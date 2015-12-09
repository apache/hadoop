import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPIAdapter.extend({
  headers: {
    Accept: 'application/json'
  },
  rmHost: 'http://localhost:1337/localhost:8088',
  tsHost: 'http://localhost:1337/localhost:8188',
  host: function() {
    return undefined
  }.property(),
  rmNamespace: 'ws/v1/cluster',
  tsNamespace: 'ws/v1/applicationhistory',
  namespace: function() {
    return undefined
  }.property(),

  urlForQuery(query, modelName) {
    if (query.is_rm) {
      this.set("host", this.rmHost);
      this.set("namespace", this.rmNamespace);
    } else {
      this.set("host", this.tsHost);
      this.set("namespace", this.tsNamespace);
    }

    var url = this._buildURL();
    url = url + '/apps/' + Converter.attemptIdToAppId(query.app_attempt_id) 
               + "/appattempts/" + query.app_attempt_id + "/containers";
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
