#!/usr/bin/python

# Run this command as:
#
# jira.sh -s https://issues.apache.org/jira -u $user -p $pw \
#   -a getIssueList --search \
#   "project in (HADOOP,HDFS,MAPREDUCE) and fixVersion = '$vers' and resolution = Fixed" \
#   | ./relnotes.py > $vers.html

import csv
import re
import sys

namePattern = re.compile(r' \([0-9]+\)')
htmlSpecialPattern = re.compile(r'[&<>\'"\n]')
quotes = {'<' : '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;',
          '&': '&amp;', '\n': '<br>'}

def clean(str):
  return re.sub(namePattern, "", str)

def formatComponents(str):
  str = re.sub(namePattern, '', str).replace("'", "")
  if str != "":
    return "(" + str + ")"
  else:
    return ""
    
def quoteHtmlChar(m):
  return quotes[m.group(0)]

def quoteHtml(str):
  return re.sub(htmlSpecialPattern, quoteHtmlChar, str)

reader = csv.reader(sys.stdin, skipinitialspace=True)

# throw away number of issues
reader.next()

# read the columns
columns = reader.next()

key = columns.index('Key')
type = columns.index('Type')
priority = columns.index('Priority')
assignee = columns.index('Assignee')
reporter = columns.index('Reporter')
summary = columns.index('Summary')
description = columns.index('Description')
components = columns.index('Components')

print "<html><body><ul>"

for row in reader:
  print \
    '<li> <a href="https://issues.apache.org/jira/browse/%s">%s</a>.\n' \
    '     %s %s reported by %s and fixed by %s %s<br>\n' \
    '     <b>%s</b><br>\n' \
    '     <blockquote>%s</blockquote></li>\n' \
    % (row[key], row[key], clean(row[priority]), clean(row[type]).lower(), 
       row[reporter], row[assignee], formatComponents(row[components]),
       quoteHtml(row[summary]), quoteHtml(row[description]))

print "</ul>\n</body></html>"
