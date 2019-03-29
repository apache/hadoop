module.exports = function(config){
  config.set({

    basePath : '../../../',

    files : [
      'target/generated-sources/vendor/angular**/**.min.js',
      'target/generated-sources/vendor/angular-mocks/angular-mocks.js',
      'src/main/javascript/**/*.js',
      'src/test/javascript/**/*Spec.js',
      'src/test/javascript/**/!(karma.conf).js'
    ],

    autoWatch : true,

    frameworks: ['jasmine'],

                 browsers: ['PhantomJS'],

    plugins : [
            'karma-chrome-launcher',
            'karma-firefox-launcher',
            'karma-phantomjs-launcher',
            'karma-jasmine',
            'karma-junit-reporter'
            ],

    junitReporter : {
      outputFile: 'target/test_out/unit.xml',
        suite: 'src/test/javascript'
    }

  });
};
