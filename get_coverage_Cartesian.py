from html.parser import HTMLParser
import csv
import os
import subprocess
import shutil
import fileinput
from datetime import datetime

jacoco_path = '/target/site/jacoco/index.html'
current_datetime = datetime.now().strftime("%m.%d.%y_%H:%M:%S")
log_folder = f"hadoop_puts_logs_{current_datetime}/"
os.makedirs(log_folder)

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_total_td = False
        self.values = []

    def handle_starttag(self, tag, attrs):
        if tag == "td" and ("class", "bar") in attrs:
            self.in_total_td = True

    def handle_data(self, data):
        if self.in_total_td:
            value = data.strip().replace(",", "")
            self.values.append(value)

    def handle_endtag(self, tag):
        if self.in_total_td and tag == "td":
            self.in_total_td = False

def get_values_from_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
        parser = MyHTMLParser()
        parser.feed(html_content)
        return parser.values

def get_coverage_details_from_html(file_path):
    values = get_values_from_html_file(file_path)
#     print(values)
    missed_instructions, total_instructions = map(int, values[0].split(" of "))
    missed_branches, total_branches = map(int, values[1].split(" of "))
    covered_Instructions = total_instructions-missed_instructions
    covered_Branches = total_branches-missed_branches

#     print("Value of covered_Instructions:", covered_Instructions)
#     print("Value of total Instructions:", total_instructions)
#     print("Value of covered_Branches:", covered_Branches)
#     print("Value of total Branches:", total_branches)
    return [covered_Instructions, total_instructions, covered_Branches, total_branches]

def getTestRunNumber(logFile):
   with open(logFile) as log:
        for line in log:
            if "Tests run:" in line:
#             Tests run: 192, Failures: 44, Errors: 16, Skipped: 96
                all=line.strip().split(', ')
                testsRun=int(all[0].split(': ')[1])
                failures=int(all[1].split(': ')[1])
                errors=int(all[2].split(': ')[1])
                skipped=int(all[3].split(': ')[1])
                passed = testsRun - failures - errors - skipped
                return [passed, failures, errors, skipped]
def getTestParamNumber(logFile):
# Add logic to also get the number of initial and resulting combination parameters
# Number of unique values for each parameter:4 x 2 x 2 x 4 x 3 = 192
    with open(logFile) as log:
        for line in log:
            if "Number of unique values for each parameter:" in line:
                # Number of unique values for each parameter:4 x 2 x 2 x 4 x 3 = 192
                paramData=line.strip().split("Number of unique values for each parameter:")[1]
                return paramData

def runForAll(csv_file):
    with open(csv_file, newline='') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            test_name = row['Fully-Qualified Test Name']
            module_path = row['Module Path']
            testFullPath=module_path + "/src/test/java/" + test_name.replace('.', '/').split('#')[0] + ".java"
            command = f"mvn test -Dtest={test_name}[\\*] -pl {module_path}"
            log_file = f"{log_folder}{test_name.replace('#', '_')}.log"
            print("Running command:", command)
            with open(log_file, 'w') as log:
                subprocess.run(command, shell=True, stdout=log, stderr=subprocess.STDOUT)
            print("CoverageData for test :",test_name, get_coverage_details_from_html(module_path + jacoco_path))
            print("Tests Run data for test:", test_name, getTestRunNumber(log_file))

            coverageData = get_coverage_details_from_html(module_path + jacoco_path)
            runData = getTestRunNumber(log_file)

            clean_jacoco(module_path)

            #already modified to run with Cartesian
            modify_test(testFullPath)

            #Now we should run it again
            log_file2 = f"{log_folder}{test_name.replace('#', '_')}2.log"
            print("Running command:", command)
            with open(log_file2, 'w') as log:
                subprocess.run(command, shell=True, stdout=log, stderr=subprocess.STDOUT)
            print("CoverageData2 for test :",test_name, get_coverage_details_from_html(module_path + jacoco_path))
            print("Tests Run data2 for test:", test_name, getTestRunNumber(log_file2))
            print("Param data for test:", test_name, getTestParamNumber(log_file2))
            paramNumberInfo=getTestParamNumber(log_file2)

            coverageData_C=get_coverage_details_from_html(module_path + jacoco_path)
            runData_C = getTestRunNumber(log_file2)
            writeCSV(test_name, coverageData, runData, coverageData_C, runData_C, paramNumberInfo)

            clean_jacoco(module_path)
            modify_test(testFullPath, False)

def clean_jacoco(modulePath):
    shutil.rmtree(modulePath + "/target/site/jacoco")
    os.remove(modulePath + "/target/jacoco.exec")

def modify_test(file_path, makeCartesian=True):
    with fileinput.FileInput(file_path, inplace=True) as file:
        for line in file:
            if makeCartesian:
                print(line.replace('import org.junit.runners.Parameterized;', 'import edu.illinois.Parameterized;'), end='')
            else:
                print(line.replace('import edu.illinois.Parameterized;', 'import org.junit.runners.Parameterized;'), end='')

def modify_test_back(file_path):
    with fileinput.FileInput(file_path, inplace=True) as file:
        for line in file:
            # Modify the test file as needed
            # For example, replace a line with a new one
            print(line.replace('import org.junit.runners.Parameterized;', 'import edu.illinois.Parameterized;'), end='')

def writeCSV(test_name, coverageData, runData, coverageData_C, runData_C, paramNumberInfo):
    csv_filename = f"put_data.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["Project URL", "Module Path", "Fully-Qualified Test Name","ParameterCombinations",
                      "CoveredInstructions", "CoveredBranches", "Passed", "Failure", "Errors", "Skipped",
                      "CoveredInstructions_C", "CoveredBranches_C", "Passed_C", "Failure_C", "Errors_C", "Skipped_C"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            "Project URL": "Hadoop",
            "Module Path": "module_path",
            "Fully-Qualified Test Name": test_name,
            "ParameterCombinations": paramNumberInfo,
            "CoveredInstructions": coverageData[0],
            "CoveredBranches": coverageData[2],
            "Passed": runData[0],
            "Failure": runData[1],
            "Errors": runData[2],
            "Skipped": runData[3],
            "CoveredInstructions_C": coverageData_C[0],
            "CoveredBranches_C": coverageData_C[2],
            "Passed_C": runData_C[0],
            "Failure_C": runData_C[1],
            "Errors_C": runData_C[2],
            "Skipped_C": runData_C[3],
        })



if __name__ == "__main__":
    module_path = 'hadoop-common-project/hadoop-common'
    clean_jacoco(module_path)
    file_path = module_path + jacoco_path
    csv_file = 'hadoop_common_puts_2.csv'
#     print(get_coverage_details_from_html(file_path))
    runForAll(csv_file)

