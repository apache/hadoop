import Ember from 'ember';
import config from './config/environment';

var Router = Ember.Router.extend({
  location: config.locationType
});

Router.map(function() {
  this.route('yarnApps');
  this.route('yarnQueue', { path: '/yarnQueue/:queue_name' });
  this.route('clusterOverview');
  this.route('yarnApp', { path: '/yarnApp/:app_id' });
  this.route('yarnAppAttempt', { path: '/yarnAppAttempt/:app_attempt_id'});
});

export default Router;
