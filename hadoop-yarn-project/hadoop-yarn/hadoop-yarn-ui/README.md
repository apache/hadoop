# Yarn-ui
*This is a WIP project, nobody should use it in production.*

## Prerequisites

You will need the following things properly installed on your computer.

* Install Node.js with NPM: https://nodejs.org/download/
* After Node.js installed, install bower: `npm install -g bower`.
* Install Ember-cli: `npm install -g ember-cli`

## Installation

* Goto root directory of yarn-ui project: `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-ui`
* `npm install && bower install`, it will take a while to finish.

## Try it

* Packaging and deploying Hadoop in this branch (You can use latest trunk after YARN-4417 committed to trunk)
* Modify `app/adapters/yarn-app.js`, change `host` to your YARN RM web address
* If you running YARN RM in your localhost, you should install `npm install -g corsproxy` and run `corsproxy` to avoid CORS errors. More details: `https://www.npmjs.com/package/corsproxy`. And the `host` of `app/adapters/yarn-app.js` should start with `localhost:1337`.
* Run `ember server`
* Visit your app at [http://localhost:4200](http://localhost:4200).

