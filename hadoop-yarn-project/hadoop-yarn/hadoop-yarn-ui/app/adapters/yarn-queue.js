import DS from 'ember-data';
import Config from 'yarn-ui/config';

export default DS.JSONAPIAdapter.extend({
  headers: {
    Accept: 'application/json'
  },
  host: 'http://localhost:1337/' + Config.RM_HOST + ':' + Config.RM_PORT, // configurable
  namespace: 'ws/v1/cluster', // common const
  pathForType(modelName) {
    return 'scheduler'; // move to some common place, return path by modelname.
  },
  ajax(url, method, hash) {
    hash = hash || {};
    hash.crossDomain = true;
    hash.xhrFields = {withCredentials: true};
    hash.targetServer = "RM";
    return this._super(url, method, hash); 
  }
});
