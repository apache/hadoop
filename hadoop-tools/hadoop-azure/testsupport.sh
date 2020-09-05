conffile=src/test/resources/azure-auth-keys.xml
bkpconffile=src/test/resources/azure-auth-keys_BKP.xml
testresultsregex="Results:(\n|.)*?Tests run:"
testresultsfilename=
starttime=

validate() {
  if [ -z $scenario ]; then
   echo "Exiting. scenario cannot be empty"
   exit
  fi
  propertiessize=${#properties[@]}
  valuessize=${#values[@]}
  if [ $propertiessize -lt 1 ] || [ $valuessize -lt 1 ] || [ $propertiessize -ne $valuessize ]; then
    echo "Exiting. Both properties and values arrays has to be populated and of same size. Please check for scenario $scenario"
    exit
  fi
}

checkdependancies() {
  command -v pcregrep &>/dev/null
  if [[ "${?}" -ne 0 ]]; then
    echo "Exiting. pcregrep is required to run the script."
    exit
  fi
  command -v xmlstarlet &>/dev/null
  if [[ "${?}" -ne 0 ]]; then
    echo "Exiting. xmlstarlet is required to run the script."
    exit
  fi
}

changeconf() {
  xmlstarlet ed -P -L -d "/configuration/property[name='$1']" $conffile
  xmlstarlet ed -P -L -s /configuration -t elem -n propertyTMP -v "" -s /configuration/propertyTMP -t elem -n name -v "$1" -r /configuration/propertyTMP -v property $conffile
  xmlstarlet ed -P -L -s "/configuration/property[name='$1']" -t elem -n value -v "$2" $conffile
}

testwithconfs() {
  propertiessize=${#properties[@]}
  valuessize=${#values[@]}
  if [ $propertiessize -ne $valuessize ]; then
    echo "Exiting. Number of properties and values differ for $scenario"
    exit
  fi
  for ((i = 0; i < $propertiessize; i++)); do
    key=${properties[$i]}
    val=${values[$i]}
    changeconf $key $val
  done
  mvn -T 1C -Dparallel-tests=abfs -Dscale -DtestsThreadCount=8 clean verify >>$testlogfilename
}

summary() {
  echo "" >>$testresultsfilename
  echo $scenario >>$testresultsfilename
  echo ======================== >>$testresultsfilename
  pcregrep -M "$testresultsregex" "$testlogfilename" >>$testresultsfilename
  printf "\n----- Test results -----\n"
  pcregrep -M "$testresultsregex" "$testlogfilename"

  secondstaken=$(($ENDTIME - $STARTTIME))
  mins=$((secondstaken / 60))
  secs=$((secondstaken % 60))
  printf "\nTime taken: $mins mins $secs secs.\n"
  echo "Find test logs for the scenario ($scenario) in: $testlogfilename"
  echo "Find consolidated test results in: $testresultsfilename"
  echo "----------"
}

runtestwithconfs() {
  validate
  cp $conffile $bkpconffile
  if [ -z "$starttime" ]; then
    starttime=$(date +"%Y-%m-%d_%H-%M-%S")
    testresultsfilename="testlogs/Test-$starttime-Results.log"
    checkdependancies
    mvn clean install -DskipTests
  fi
  STARTTIME=$(date +%s)
  testlogfilename="testlogs/Test-$starttime-Logs-$scenario.log"
  printf "\nRunning the scenario: $scenario..."
  testwithconfs
  ENDTIME=$(date +%s)
  summary
  scenario=
  properties=()
  values=()
  cp $bkpconffile $conffile
  rm -rf $bkpconffile
}
