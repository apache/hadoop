import { moduleForModel, test } from 'ember-qunit';

moduleForModel('yarn-app', 'Unit | Model | yarn app', {
  // Specify the other units that are required for this test.
  needs: []
});

test('it exists', function(assert) {
  var model = this.subject();
  // var store = this.store();
  assert.ok(!!model);
});
