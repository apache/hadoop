#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
  ulimit -n 1024
  ### Setup some variables.  
  ### Read variables from properties file
  BASEDIR=$(dirname $0)
  . ${BASEDIR}/../test-patch.properties
  BASEDIR=$(cd ${BASEDIR};pwd)/../../../ 
  DATE_CMD='date +"%m-%d-%Y:%T"'
  CURL_OPTS="-s --connect-timeout 10 --retry 2 --retry-delay 2"
  JIRA_URL=${JIRA_URL:-"https://issues.apache.org/jira"}
  PROJECT="HADOOP"

  usage() {
     echo "
     usage : $0 <options>
       Required Options:
         --patch-dir             patch dir
         --findbugs-home         path to findbugs home
         --forrest-home          path to forrest home
         --eclipse-home          path to eclipse home
        Optional Options:
         --jenkins               set jenkins mode
         --patch-file            pass the patch file in developer mode
        Required Options when set to jenkins mode:
         --jira-cli              path to jira cli tools 
         --jira-password         jira access password
         --issue-num             issue number to test
         --attach-id             patch attachment id to test
      "
      exit 1
  }

  echo_banner() {
    echo ""
    echo ""
    echo "======================================================================"
    echo "======================================================================"
    echo " ${*} "   
    echo "======================================================================"
    echo "======================================================================"
    echo ""
    echo ""
  }

  parseArgs() {
  
  OPTS=$(getopt \
    -n $0 \
    -o '' \
    -l 'patch-dir:' \
    -l 'jira-cli:' \
    -l 'findbugs-home:' \
    -l 'forrest-home:' \
    -l 'eclipse-home:' \
    -l 'jira-password:' \
    -l 'issue-num:' \
    -l 'attach-id:' \
    -l 'patch-file:' \
    -l 'jenkins:' \
    -- "$@")

  if [ $? != 0 ]; then
      usage
  fi

  eval set -- "$OPTS"
  while true ; do 
    case "$1" in 
      --patch-dir)
      PATCH_DIR=${2} ; shift 2
      ;;
      --jira-cli)
      JIRA_CLI=${2} ; shift 2
      ;;
      --findbugs-home)
      FINDBUGS_HOME=${2} ; shift 2
      ;;
      --forrest-home)
      FORREST_HOME=${2} ; shift 2
      ;;
      --eclipse-home)
      ECLIPSE_HOME=${2} ; shift 2
      ;;
      --jira-password)
      JIRA_PASSWORD=${2} ; shift 2
      ;;
      --issue-num)
      ISSUE_NUM=${2} ; shift 2
      ;;
      --attach-id)
      ATTACH_ID=${2} ; shift 2
      ;;
      --patch-file)
      PATCH_FILE=${2} ; shift 2
      ;;
      --jenkins)
      JENKINS=${2} ; shift 2
      ;;
      --)
      shift ; break
      ;;
      *) 
      echo "Unknows option: $1"
      usage
      exit 1
      ;;
    esac
  done
   
  [[ -n $JENKINS ]] && echo "Jenkins mode" && \
    [[ -z "$PATCH_DIR" || -z "$JIRA_CLI" || -z "$FINDBUGS_HOME" \
         || -z "$FORREST_HOME" || -z "$ECLIPSE_HOME" || -z "$JIRA_PASSWORD" \
         || -z "$ISSUE_NUM" || -z "$ATTACH_ID" ]] \
         && echo "Required Args Missing" && usage
  [[ -z $JENKINS ]] && echo "Developer mode"  && \
    [[ -z "$PATCH_FILE" || -z "$PATCH_DIR" || -z "$FINDBUGS_HOME" \
         || -z "$FORREST_HOME" || -z "$ECLIPSE_HOME" ]] \
         && echo "Required Args Missing" && usage
   [[ ! -d "$PATCH_DIR" ]] && mkdir -p "$PATCH_DIR"

   SVN=$(which svn 2> /dev/null)
   GIT=$(which git 2> /dev/null)
   [[ -d './.git' && -n $GIT ]] && GITPROJECT="true" && REVISION=$($GIT rev-parse --short HEAD)
   [[ -d './.svn' && -n $SVN ]] && SVNPROJECT="true" && REVISION=$(svnversion)
   [[ -z $GITPROJECT && -z $SVNPROJECT ]] && echo "neither git nor an svn project" && exit 1

   CURL=$(which curl 2> /dev/null)
   GREP=$(which grep 2> /dev/null)
   PATCH=$(which patch 2> /dev/null)
   [[ -z $CURL ]] && echo "curl not in path" && exit 1
   [[ -z $PATCH ]] && echo "patch not in path" && exit 1
   [[ -z $GREP ]] && echo "grep not in path" && exit 1
   [[ -z $JENKINS ]] && JENKINS="false" 
   PATCH_URL=${JIRA_URL}/secure/attachment/${ATTACH_ID}/
   VERSION=${PROJECT}-${REVISION}
   ANT_CMD="$ANT_HOME/bin/ant -Dversion=${VERSION}"
  }

  ###############################################################################
  ### Cleanup files
  cleanupAndExit () {
    local result=$1
    if [[ $JENKINS == "true" ]] ; then
      if [ -e "$PATCH_DIR" ] ; then
        mv $PATCH_DIR $BASEDIR
      fi
    fi
    echo_banner "    Finished build."
    exit $result
  }

  ###############################################################################
  checkout () {
    echo_banner "Testing patch for ${PROJECT}-${ISSUE_NUM}."
    ### When run by a developer, if the workspace contains modifications, do not continue
    if [[ -n $GITPROJECT ]] ; then 
      status=$($GIT status)
      [[ $JENKINS == "false" ]] && [[ -n "$status" ]] && [[ $(echo $status | $GREP -c '(working directory clean)') -eq 0 ]] && \
        echo "Local modification found $status" && cleanupAndExit 1
        (cd $BASEDIR ; $GIT clean -fdx ; $GIT reset --hard )
    fi
    if [[ -n $SVNPROJECT ]] ; then 
      status=$($SVN stat)
      [[ $JENKINS == "false" ]] && [[ -n "$status" ]] && \
        echo "Local modification found $status" && cleanupAndExit 1
      (cd $BASEDIR ; $SVN revert -R ; rm -rf $($SVN status) ; $SVN up)
    fi
    return $?
  }


  ###############################################################################
  setup () {
    ### Download latest patch file (ignoring .htm and .html) when run from patch process
    if [[ $JENKINS = "true" ]] ; then
      $CURL $CURL_OPTS -o $PATCH_DIR/jira $JIRA_URL/browse/${PROJECT}-${ISSUE_NUM}
      if [[ `$GREP -c 'Patch Available' $PATCH_DIR/jira` == 0 ]] ; then
        echo "${PROJECT}-${ISSUE_NUM} is not \"Patch Available\".  Exiting."
        cleanupAndExit 0
      fi
      echo " [`$DATE_CMD`] Downloaded $PATCH_URL"
      $CURL $CURL_OPTS -o $PATCH_DIR/patch $PATCH_URL
      [[ $? -ne 0 ]] && echo "$PATCH_URL download failed" && cleanupAndExit 1
      JIRA_COMMENT="Here are the results of testing 
    $patchURL
    against revision ${REVISION}"
    else
      cp $PATCH_FILE $PATCH_DIR/patch
      [[ $? -ne 0 ]] && echo "Could not copy $PATCH_FILE to $PATCH_DIR" && cleanupAndExit 1
      echo "Patch file $PATCH_FILE copied to $PATCH_DIR"
    fi
    ### exit if warnings are NOT defined in the properties file
    if [ -z "$OK_FINDBUGS_WARNINGS" ] || [[ -z "$OK_JAVADOC_WARNINGS" ]] || [[ -z $OK_RELEASEAUDIT_WARNINGS ]]; then
    echo "Please define the following properties in test-patch.properties file"
    echo "OK_FINDBUGS_WARNINGS"
    echo "OK_RELEASEAUDIT_WARNINGS"
    echo "OK_JAVADOC_WARNINGS"
    cleanupAndExit 1
    fi
    echo_banner " Pre-build codebase to verify stability and javac warnings" 
    ### Do not call releaseaudit when run by a developer
    if [[ $JENKINS = "true" ]] ; then
      echo "$ANT_CMD -Dforrest.home=${FORREST_HOME} -D${PROJECT}PatchProcess= releaseaudit > $PATCH_DIR/currentReleaseAuditWarnings.txt 2>&1"
      $ANT_CMD -Dforrest.home=${FORREST_HOME} -D${PROJECT}PatchProcess= releaseaudit > $PATCH_DIR/currentReleaseAuditWarnings.txt 2>&1
    fi
    echo "$ANT_CMD -Djavac.args=-Xlint -Xmaxwarns 1000 -Dforrest.home=${FORREST_HOME} -D${PROJECT_NAME}PatchProcess= clean tar > $PATCH_DIR/currentJavacWarnings.txt 2>&1"
    $ANT_CMD -Djavac.args="-Xlint -Xmaxwarns 1000" -Dforrest.home=${FORREST_HOME} -D${PROJECT_NAME}PatchProcess= clean  tar > $PATCH_DIR/currentJavacWarnings.txt 2>&1
    [[ $? -ne 0 ]] && echo "compilation is broken?" && cleanupAndExit 1
  }

  ###############################################################################
  ### Check for @author tags in the patch
  checkAuthor () {
    echo_banner "    Checking there are no @author tags in the patch."
    authorTags=`$GREP -c -i '@author' $PATCH_DIR/patch`
    echo "There appear to be $authorTags @author tags in the patch."
    if [[ $authorTags != 0 ]] ; then
      JIRA_COMMENT="$JIRA_COMMENT

      -1 @author.  The patch appears to contain $authorTags @author tags which the Hadoop community has agreed to not allow in code contributions."
      return 1
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 @author.  The patch does not contain any @author tags."
    return 0
  }

  ###############################################################################
  ### Check for tests in the patch
  checkTests () {
    echo_banner "    Checking there are new or changed tests in the patch."
    testReferences=`$GREP -c -i '/test' $PATCH_DIR/patch`
    echo "There appear to be $testReferences test files referenced in the patch."
    if [[ $testReferences == 0 ]] ; then
      if [[ $JENKINS == "true" ]] ; then
        patchIsDoc=`$GREP -c -i 'title="documentation' $PATCH_DIR/jira`
        if [[ $patchIsDoc != 0 ]] ; then
          echo "The patch appears to be a documentation patch that doesn\'t require tests."
          JIRA_COMMENT="$JIRA_COMMENT

      +0 tests included.  The patch appears to be a documentation patch that doesn't require tests."
          return 0
        fi
      fi
      JIRA_COMMENT="$JIRA_COMMENT

      -1 tests included.  The patch doesn't appear to include any new or modified tests.
                          Please justify why no tests are needed for this patch."
      return 1
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 tests included.  The patch appears to include $testReferences new or modified tests."
    return 0
  }

  ###############################################################################
  ### Attempt to apply the patch
  applyPatch () {
    echo_banner "    Applying patch."
    $PATCH -t -l -E -p0 < $PATCH_DIR/patch
    [[ $? -ne 0 ]] && $PATCH -t -l -E -p1 < $PATCH_DIR/patch
    if [ $? -ne 0 ] ; then
      echo "PATCH APPLICATION FAILED"
      JIRA_COMMENT="$JIRA_COMMENT

      -1 patch.  The patch command could not apply the patch."
      return 1
    fi
    return 0
  }


  ###############################################################################
  ### Check there are no javadoc warnings
  checkJavadocWarnings () {
    echo_banner "    Determining number of patched javadoc warnings."
    echo "$ANT_CMD -DHadoopPatchProcess= clean javadoc | tee $PATCH_DIR/patchJavadocWarnings.txt"
    $ANT_CMD -DHadoopPatchProcess= clean javadoc | tee $PATCH_DIR/patchJavadocWarnings.txt
    javadocWarnings=`$GREP -o '\[javadoc\] [0-9]* warning' $PATCH_DIR/patchJavadocWarnings.txt | awk '{total += $2} END {print total}'`
    echo ""
    echo ""
    echo "There appear to be $javadocWarnings javadoc warnings generated by the patched build."

    ### if current warnings greater than OK_JAVADOC_WARNINGS
    if [[ $javadocWarnings > $OK_JAVADOC_WARNINGS ]] ; then
      JIRA_COMMENT="$JIRA_COMMENT

      -1 javadoc.  The javadoc tool appears to have generated `expr $(($javadocWarnings-$OK_JAVADOC_WARNINGS))` warning messages."
      return 1
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 javadoc.  The javadoc tool did not generate any warning messages."
  return 0
  }

  ###############################################################################
  ### Check there are no changes in the number of Javac warnings
  checkJavacWarnings () {
    echo_banner "    Determining number of patched javac warnings."
    echo "$ANT_CMD -Djavac.args=-Xlint -Xmaxwarns 1000 -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= tar > $PATCH_DIR/patchJavacWarnings.txt 2>&1"
    $ANT_CMD -Djavac.args="-Xlint -Xmaxwarns 1000" -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= tar > $PATCH_DIR/patchJavacWarnings.txt 2>&1
    if [[ $? != 0 ]] ; then
      JIRA_COMMENT="$JIRA_COMMENT

      -1 javac.  The patch appears to cause tar ant target to fail."
      return 1
    fi

    ### Compare current codebase and patch javac warning numbers
    if [[ -f $PATCH_DIR/patchJavacWarnings.txt ]] ; then
      currentJavacWarnings=`$GREP -o '\[javac\] [0-9]* warning' $PATCH_DIR/currentJavacWarnings.txt | awk '{total += $2} END {print total}'`
      patchJavacWarnings=`$GREP -o '\[javac\] [0-9]* warning' $PATCH_DIR/patchJavacWarnings.txt | awk '{total += $2} END {print total}'`
      echo "There appear to be $currentJavacWarnings javac compiler warnings before the patch and $patchJavacWarnings javac compiler warnings after applying the patch."
      if [[ $patchJavacWarnings != "" && $currentJavacWarnings != "" ]] ; then
        if [[ $patchJavacWarnings -gt $currentJavacWarnings ]] ; then
          JIRA_COMMENT="$JIRA_COMMENT

      -1 javac.  The applied patch generated $patchJavacWarnings javac compiler warnings (more than the current $currentJavacWarnings warnings)."

          return 1
        fi
      fi
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 javac.  The applied patch does not increase the total number of javac compiler warnings."
    return 0
  }

  ###############################################################################
  ### Check there are no changes in the number of release audit (RAT) warnings
  checkReleaseAuditWarnings () {
    echo_banner  " Determining number of patched release audit warnings."
    echo "$ANT_CMD -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= releaseaudit > $PATCH_DIR/patchReleaseAuditWarnings.txt 2>&1"
    ${ANT_CMD} -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= releaseaudit > $PATCH_DIR/patchReleaseAuditWarnings.txt 2>&1

    ### Compare current and patch release audit warning numbers
    if [[ -f $PATCH_DIR/patchReleaseAuditWarnings.txt ]] ; then
      patchReleaseAuditWarnings=`$GREP -c '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt`
      echo ""
      echo ""
      echo "There appear to be $OK_RELEASEAUDIT_WARNINGS release audit warnings before the patch and $patchReleaseAuditWarnings release audit warnings after applying the patch."
      if [[ $patchReleaseAuditWarnings != "" && $OK_RELEASEAUDIT_WARNINGS != "" ]] ; then
        if [[ $patchReleaseAuditWarnings -gt $OK_RELEASEAUDIT_WARNINGS ]] ; then
          JIRA_COMMENT="$JIRA_COMMENT

      -1 release audit.  The applied patch generated $patchReleaseAuditWarnings release audit warnings (more than the current $OK_RELEASEAUDIT_WARNINGS warnings)."
          $GREP '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt > $PATCH_DIR/patchReleaseAuditProblems.txt
          echo "Lines that start with ????? in the release audit report indicate files that do not have an Apache license header." >> $PATCH_DIR/patchReleaseAuditProblems.txt
          JIRA_COMMENT_FOOTER="Release audit warnings: $BUILD_URL/artifact/trunk/patchprocess/patchReleaseAuditProblems.txt
  $JIRA_COMMENT_FOOTER"
          return 1
        fi
      fi
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 release audit.  The applied patch does not increase the total number of release audit warnings."
    return 0
  }

  ###############################################################################
  ### Check there are no changes in the number of Findbugs warnings
      checkFindbugsWarnings () {
        findbugs_version=`${FINDBUGS_HOME}/bin/findbugs -version`
        echo_banner "    Determining number of patched Findbugs warnings."
        echo "$ANT_CMD -Dfindbugs.home=${FINDBUGS_HOME} -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= findbugs"
        $ANT_CMD -Dfindbugs.home=${FINDBUGS_HOME} -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= findbugs
        if [ $? != 0 ] ; then
          JIRA_COMMENT="$JIRA_COMMENT

        -1 findbugs.  The patch appears to cause Findbugs \(version ${findbugs_version}\) to fail."
          return 1
        fi
      JIRA_COMMENT_FOOTER="Findbugs warnings: $BUILD_URL/artifact/trunk/build/test/findbugs/newPatchFindbugsWarnings.html
      $JIRA_COMMENT_FOOTER"
        cp $BASEDIR/build/test/findbugs/*.xml $PATCH_DIR/patchFindbugsWarnings.xml
        $FINDBUGS_HOME/bin/setBugDatabaseInfo -timestamp "01/01/2000" \
          $PATCH_DIR/patchFindbugsWarnings.xml \
          $PATCH_DIR/patchFindbugsWarnings.xml
        findbugsWarnings=`$FINDBUGS_HOME/bin/filterBugs -first "01/01/2000" $PATCH_DIR/patchFindbugsWarnings.xml \
          $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.xml | awk '{print $1}'`
        $FINDBUGS_HOME/bin/convertXmlToText -html \
          $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.xml \
          $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.html
        cp $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.html $PATCH_DIR/newPatchFindbugsWarnings.html
        cp $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.xml $PATCH_DIR/newPatchFindbugsWarnings.xml

        ### if current warnings greater than OK_FINDBUGS_WARNINGS
        if [[ $findbugsWarnings > $OK_FINDBUGS_WARNINGS ]] ; then
          JIRA_COMMENT="$JIRA_COMMENT

        -1 findbugs.  The patch appears to introduce `expr $(($findbugsWarnings-$OK_FINDBUGS_WARNINGS))` new Findbugs (version ${findbugs_version}) warnings."
          return 1
        fi
        JIRA_COMMENT="$JIRA_COMMENT

        +1 findbugs.  The patch does not introduce any new Findbugs (version ${findbugs_version}) warnings."
        return 0
      }

  ###############################################################################
  ### Run the test-core target
  runCoreTests () {
    echo_banner "    Running core tests."
    
    ### Kill any rogue build processes from the last attempt
    ps auxwww | $GREP HadoopPatchProcess | awk '{print $2}' | xargs -t -I {} kill -9 {} > /dev/null

    echo "$ANT_CMD -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes -Dcompile.c++=yes -Dforrest.home=$FORREST_HOME create-c++-configure test-core"
    $ANT_CMD -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes -Dcompile.c++=yes -Dforrest.home=$FORREST_HOME create-c++-configure test-core
    if [[ $? != 0 ]] ; then
      failed_tests=`grep -l "<failure" build/test/*.xml | sed -e "s|build/test/TEST-|                  |g" | sed -e "s|\.xml||g"`
      JIRA_COMMENT="$JIRA_COMMENT

      -1 core tests.  The patch failed these core unit tests:
      $failed_tests"
      return 1
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 core tests.  The patch passed core unit tests."
    return 0
  }

  ###############################################################################
  ### Run the test-contrib target
  runContribTests () {
    echo_banner "    Running contrib tests."
    ### Kill any rogue build processes from the last attempt
    ps -auxwww | $GREP HadoopPatchProcess | awk '{print $2}' | xargs -t -I {} kill -9 {} > /dev/null

    echo "$ANT_CMD -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes test-contrib"
    $ANT_CMD -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes test-contrib
    if [[ $? != 0 ]] ; then
      JIRA_COMMENT="$JIRA_COMMENT

      -1 contrib tests.  The patch failed contrib unit tests."
      return 1
    fi
    JIRA_COMMENT="$JIRA_COMMENT

      +1 contrib tests.  The patch passed contrib unit tests."
    return 0
  }

  ###############################################################################
  ### Submit a comment to the defect's Jira
  submitJiraComment () {
    local result=$1
    ### Do not output the value of JIRA_COMMENT_FOOTER when run by a developer
    if [[  $JENKINS == "false" ]] ; then
      JIRA_COMMENT_FOOTER=""
    fi
    if [[ $result == 0 ]] ; then
      comment="+1 overall.  $JIRA_COMMENT

  $JIRA_COMMENT_FOOTER"
    else
      comment="-1 overall.  $JIRA_COMMENT

  $JIRA_COMMENT_FOOTER"
    fi
    ### Output the test result to the console
    echo "



  $comment"  

    if [[ $JENKINS == "true" ]] ; then
      echo_banner "    Adding comment to Jira."

      ### Update Jira with a comment
      export USER=jenkins
      $JIRA_CLI -s ${JIRA_URL} login hadoopqa $JIRA_PASSWD
      $JIRA_CLI -s ${JIRA_URL} comment ${PROJECT}-${ISSUE_NUM} "$comment"
      $JIRA_CLI -s ${JIRA_URL} logout
    fi
  }


  parseArgs $@
  checkout
  [[ $? -ne 0 ]] && cleanupAndExit 1
  setup
  checkAuthor
  RESULT=$?
  checkTests
  (( RESULT = RESULT + $? ))
  applyPatch
  if [[ $? != 0 ]] ; then
    submitJiraComment 1
    cleanupAndExit 1
  fi
  checkJavadocWarnings
  (( RESULT = $RESULT + $? ))
  checkJavacWarnings
  (( RESULT = $RESULT + $? ))
  checkStyle
  (( RESULT = $RESULT + $? ))
  checkFindbugsWarnings
  (( RESULT = $RESULT + $? ))
  ### Do not call these when run by a developer 
  if [[ $JENKINS == "true" ]] ; then
    checkReleaseAuditWarnings
    (( RESULT = $RESULT + $? ))
    runCoreTests
    (( RESULT = $RESULT + $? ))
    runContribTests
    (( RESULT = $RESULT + $? ))
  fi
  JIRA_COMMENT_FOOTER="Test results\:$BUILD_URL/testReport/\"
  $JIRA_COMMENT_FOOTER"

  submitJiraComment $RESULT
  cleanupAndExit $RESULT
