import json

try:
    with open('/opt/metadata/node.json') as nodeFile:
        node = json.load(nodeFile)

    attributes = node['attributes']
    yarn = attributes['YARN']

    hostGroupName = node['hostGroup']

    print("NODE_ATTRIBUTE:HostGroup,STRING," + hostGroupName)

    for i in yarn:
        print("NODE_ATTRIBUTE:" + i + ",STRING," + yarn[i])

except IOError:
    print("node.json file does not exist!")

except ValueError:
    print("Not in JSON Format!")

except KeyError as k:
    print(k.message + " field name does not exist!")

except TypeError as t:
    if hostGroupName is None:
        print("hostGroup is null!")
    else:
        print("attribute value is null!")
