// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pipeline {

    agent {
        label 'Hadoop'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '5'))
        timeout (time: 5, unit: 'HOURS')
        timestamps()
        checkoutToSubdirectory('src')
    }

    environment {
        SOURCEDIR = 'src'
        // will also need to change notification section below
        PATCHDIR = 'out'
        DOCKERFILE = "${SOURCEDIR}/dev-support/docker/Dockerfile"
        YETUS='yetus'
        // Branch or tag name.  Yetus release tags are 'rel/X.Y.Z'
        YETUS_VERSION='rel/0.9.0'
    }

    parameters {
        string(name: 'JIRA_ISSUE_KEY',
               defaultValue: '',
               description: 'The JIRA issue that has a patch needing pre-commit testing. Example: HADOOP-1234')
    }

    stages {
        stage ('install yetus') {
            steps {
                dir("${WORKSPACE}/${YETUS}") {
                    checkout([
                        $class: 'GitSCM',
                        branches: [[name: "${env.YETUS_VERSION}"]],
                        userRemoteConfigs: [[ url: 'https://github.com/apache/yetus.git']]]
                    )
                }
            }
        }

        stage ('precommit-run') {
            steps {
                withCredentials(
                    [usernamePassword(credentialsId: 'apache-hadoop-at-github.com',
                                  passwordVariable: 'GITHUB_PASSWORD',
                                  usernameVariable: 'GITHUB_USER'),
                    usernamePassword(credentialsId: 'hadoopqa-at-asf-jira',
                                        passwordVariable: 'JIRA_PASSWORD',
                                        usernameVariable: 'JIRA_USER')]) {
                        sh '''#!/usr/bin/env bash

                        set -e

                        TESTPATCHBIN="${WORKSPACE}/${YETUS}/precommit/src/main/shell/test-patch.sh"

                        # this must be clean for every run
                        if [[ -d "${WORKSPACE}/${PATCHDIR}" ]]; then
                          rm -rf "${WORKSPACE}/${PATCHDIR}"
                        fi
                        mkdir -p "${WORKSPACE}/${PATCHDIR}"

                        # if given a JIRA issue, process it. If CHANGE_URL is set
                        # (e.g., Github Branch Source plugin), process it.
                        # otherwise exit, because we don't want Hadoop to do a
                        # full build.  We wouldn't normally do this check for smaller
                        # projects. :)
                        if [[ -n "${JIRA_ISSUE_KEY}" ]]; then
                            YETUS_ARGS+=("${JIRA_ISSUE_KEY}")
                        elif [[ -z "${CHANGE_URL}" ]]; then
                            echo "Full build skipped" > "${WORKSPACE}/${PATCHDIR}/report.html"
                            exit 0
                        fi

                        YETUS_ARGS+=("--patch-dir=${WORKSPACE}/${PATCHDIR}")

                        # where the source is located
                        YETUS_ARGS+=("--basedir=${WORKSPACE}/${SOURCEDIR}")

                        # our project defaults come from a personality file
                        # which will get loaded automatically by setting the project name
                        YETUS_ARGS+=("--project=hadoop")

                        # lots of different output formats
                        YETUS_ARGS+=("--brief-report-file=${WORKSPACE}/${PATCHDIR}/brief.txt")
                        YETUS_ARGS+=("--console-report-file=${WORKSPACE}/${PATCHDIR}/console.txt")
                        YETUS_ARGS+=("--html-report-file=${WORKSPACE}/${PATCHDIR}/report.html")

                        # enable writing back to Github
                        YETUS_ARGS+=(--github-password="${GITHUB_PASSWORD}")
                        YETUS_ARGS+=(--github-user=${GITHUB_USER})

                        # enable writing back to ASF JIRA
                        YETUS_ARGS+=(--jira-password="${JIRA_PASSWORD}")
                        YETUS_ARGS+=(--jira-user="${JIRA_USER}")

                        # auto-kill any surefire stragglers during unit test runs
                        YETUS_ARGS+=("--reapermode=kill")

                        # set relatively high limits for ASF machines
                        # changing these to higher values may cause problems
                        # with other jobs on systemd-enabled machines
                        YETUS_ARGS+=("--proclimit=5500")
                        YETUS_ARGS+=("--dockermemlimit=20g")

                        # -1 findbugs issues that show up prior to the patch being applied
                        YETUS_ARGS+=("--findbugs-strict-precheck")

                        # rsync these files back into the archive dir
                        YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,findbugsXml.xml")

                        # URL for user-side presentation in reports and such to our artifacts
                        # (needs to match the archive bits below)
                        YETUS_ARGS+=("--build-url-artifacts=artifact/out")

                        # plugins to enable
                        YETUS_ARGS+=("--plugins=all")

                        # use Hadoop's bundled shelldocs
                        YETUS_ARGS+=("--shelldocs=/testptch/hadoop/dev-support/bin/shelldocs")

                        # don't let these tests cause -1s because we aren't really paying that
                        # much attention to them
                        YETUS_ARGS+=("--tests-filter=checkstyle")

                        # run in docker mode and specifically point to our
                        # Dockerfile since we don't want to use the auto-pulled version.
                        YETUS_ARGS+=("--docker")
                        YETUS_ARGS+=("--dockerfile=${DOCKERFILE}")

                        # effectively treat dev-suport as a custom maven module
                        YETUS_ARGS+=("--skip-dir=dev-support")

                        # help keep the ASF boxes clean
                        YETUS_ARGS+=("--sentinel")

                        "${TESTPATCHBIN}" "${YETUS_ARGS[@]}"
                        '''
                }
            }
        }

    }

    post {
        always {
          script {
            // Yetus output
            archiveArtifacts "${env.PATCHDIR}/**"
            // Publish the HTML report so that it can be looked at
            // Has to be relative to WORKSPACE.
            publishHTML (target: [
                          allowMissing: true,
                          keepAll: true,
                          alwaysLinkToLastBuild: true,
                          // Has to be relative to WORKSPACE
                          reportDir: "${env.PATCHDIR}",
                          reportFiles: 'report.html',
                          reportName: 'Yetus Report'
            ])
            // Publish JUnit results
            try {
                junit "${env.SOURCEDIR}/**/target/surefire-reports/*.xml"
            } catch(e) {
                echo 'junit processing: ' + e.toString()
            }
          }
        }

        // Jenkins pipeline jobs fill slaves on PRs without this :(
        cleanup() {
            script {
                sh '''
                    # See YETUS-764
                    if [ -f "${WORKSPACE}/${PATCHDIR}/pidfile.txt" ]; then
                      echo "test-patch process appears to still be running: killing"
                      kill `cat "${WORKSPACE}/${PATCHDIR}/pidfile.txt"` || true
                      sleep 10
                    fi
                    if [ -f "${WORKSPACE}/${PATCHDIR}/cidfile.txt" ]; then
                      echo "test-patch container appears to still be running: killing"
                      docker kill `cat "${WORKSPACE}/${PATCHDIR}/cidfile.txt"` || true
                    fi
                    # See HADOOP-13951
                    chmod -R u+rxw "${WORKSPACE}"
                    '''
                deleteDir()
            }
        }
    }
}