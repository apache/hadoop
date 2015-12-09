import Ember from 'ember';
import ChartsMixin from '../../../mixins/charts';
import { module, test } from 'qunit';

module('Unit | Mixin | charts');

// Replace this with your real tests.
test('it works', function(assert) {
  var ChartsObject = Ember.Object.extend(ChartsMixin);
  var subject = ChartsObject.create();
  assert.ok(subject);
});
