import yaml

import sys
import xml.etree.ElementTree as ET

if len(sys.argv) != 3:
    print("Usage: yml2xml.py <yml file> <xml file>")
    sys.exit(1)

with open(sys.argv[1], 'r') as f:
    yml = yaml.load(f, Loader=yaml.FullLoader)


def flat_map(src, target=None, prefix=""):
    if target is None:
        target = {}
    for k, v in src.items():
        if type(v) is dict:
            flat_map(v, target, prefix + k + ".")
        else:
            target[prefix+k] = v
    return target


def dict2xml(dic):
    content = ""
    for item in dic.items():
        content += f"""
    <property>
        <name>{item[0]}</name>
        <value>{item[1]}</value>
    </property>
        """
        pass
    return f"""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    {content}
</configuration>"""


flatten_dict = flat_map(yml)
xml = dict2xml(flatten_dict)

with open(sys.argv[2], 'w') as f:
    f.write(xml)
