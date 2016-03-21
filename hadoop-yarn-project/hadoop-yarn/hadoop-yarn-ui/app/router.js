import Ember from 'ember';
import config from './config/environment';

var Router = Ember.Router.extend({
  location: config.locationType
});

Router.map(function() {
  this.route('yarnApps');
  this.route('yarnNodes');
  this.route('yarnNode', { path: '/yarnNode/:node_id/:node_addr' });
  this.route('yarnNodeApps', { path: '/yarnNodeApps/:node_id/:node_addr' });
  this.route('yarnNodeApp',
      { path: '/yarnNodeApp/:node_id/:node_addr/:app_id' });
  this.route('yarnNodeContainers',
      { path: '/yarnNodeContainers/:node_id/:node_addr' });
  this.route('yarnNodeContainer',
      { path: '/yarnNodeContainer/:node_id/:node_addr/:container_id' });
  this.route('yarnContainerLog', { path:
      '/yarnContainerLog/:node_id/:node_addr/:container_id/:filename' });
  this.route('yarnQueue', { path: '/yarnQueue/:queue_name' });
  this.route('clusterOverview');
  this.route('yarnApp', { path: '/yarnApp/:app_id' });
  this.route('yarnAppAttempt', { path: '/yarnAppAttempt/:app_attempt_id'});
  this.route('error');
  this.route('notfound', { path: '*:' });
});

export default Router;
