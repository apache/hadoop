import os
import re

def trim_right_spaces(text):
    return text.rstrip()

def main():
    directory_name = input("./")

    for root, directories, files in os.walk(directory_name):
        for file_name in files:
            if re.match(".*\.java$", file_name):
                with open(os.path.join(root, file_name), "r") as f:
                    with open(os.path.join(root, file_name + ".tmp"), "w") as g:
                        for line in f:
                            line = trim_right_spaces(line)
                            g.write(line)
            os.replace(
                os.path.join(root, file_name + ".tmp"), os.path.join(root, file_name)
            )

if __name__ == "__main__":
    main()
