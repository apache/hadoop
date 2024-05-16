import re

# Define the path to the file
file_path = "hadoop-hdfs-project/hadoop-hdfs/target/surefire-reports/org.apache.hadoop.hdfs.server.namenode.ha.TestObserverReadProxyProvider-output.txt"
output_file_path = "./result.txt"

# Define regular expressions to match the required patterns
pattern_before = r"\[Mike\] Before for loop, the time is: (\d+)"
pattern_after = r"\[Mike\] After for loop, meaning no observer, the time is: (\d+)"

# Initialize variables to store the extracted numbers
time_before = None
time_after = None

# Open the file and read its contents
with open(file_path, 'r') as file:
    # Iterate over each line in the file
    for line in file:
        # Use regular expressions to search for the patterns
        match_before = re.search(pattern_before, line)
        match_after = re.search(pattern_after, line)
        
        # If the pattern is found, extract the number
        if match_before:
            time_before = int(match_before.group(1))
        elif match_after:
            time_after = int(match_after.group(1))

# Calculate the difference
if time_before is not None and time_after is not None:
    time_difference = time_after - time_before
    result = "Time difference: " + str(time_difference)+"\n"

    # Write the result to the output file
    with open(output_file_path, 'a') as output_file:
        output_file.write(result)
else:
    print("Failed to extract necessary information from the file.")