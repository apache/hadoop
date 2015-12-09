/* global require, module */
var EmberApp = require('ember-cli/lib/broccoli/ember-app');

module.exports = function(defaults) {
  var app = new EmberApp(defaults, {
    // Add options here
  });

  app.import("bower_components/datatables/media/css/jquery.dataTables.min.css");
  app.import("bower_components/datatables/media/js/jquery.dataTables.min.js");
  app.import("bower_components/momentjs/min/moment.min.js");
  app.import("bower_components/select2/dist/css/select2.min.css");
  app.import("bower_components/select2/dist/js/select2.min.js");

  // Use `app.import` to add additional libraries to the generated
  // output files.
  //
  // If you need to use different assets in different
  // environments, specify an object as the first parameter. That
  // object's keys should be the environment name and the values
  // should be the asset to use in that environment.
  //
  // If the library that you are including contains AMD or ES6
  // modules that you would like to import into your application
  // please specify an object with the list of modules as keys
  // along with the exports of each module as its value.

  return app.toTree();
};
