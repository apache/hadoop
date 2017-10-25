Resource Estimator Service
==========================

Resource Estimator Service can parse the history logs of production jobs, extract their resource consumption skylines in the past runs and predict their resource requirements for the new run.

## Current Status

  * Support [Hadoop YARN ResourceManager](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) logs.
  * In-memory store for parsed history resource skyline and estimation.
  * A [Linear Programming](https://github.com/optimatika/ojAlgo) based estimator.
  * Provides REST interface to parse logs, query history store and estimations.

## Upcoming features

  * UI to query history and edit and save estimations.
  * Persisent store implementation for store (either DB based or distributed key-value like HBase).
  * Integrate directly with the [Hadoop YARN Reservation System](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ReservationSystem.html) to make a recurring reservation based on the estimated resources.

Refer to the [design document](https://issues.apache.org/jira/secure/attachment/12886714/ResourceEstimator-design-v1.pdf) for more details.
