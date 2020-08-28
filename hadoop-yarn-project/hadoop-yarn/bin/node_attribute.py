import json

try:
    with open('/opt/metadata/nodeinfo.json') as nodeInfoFile:
        nodeInfo = json.load(nodeInfoFile)

    attributes = nodeInfo['attributes']
    yarn = attributes['yarn']
    yarnInstanceType = ','.join(yarn)

    hostGroupName = nodeInfo['hostGroup']

    print("NODE_ATTRIBUTE:HostGroup,STRING," + hostGroupName)
    print("NODE_ATTRIBUTE:NodeInstanceType,STRING," + yarnInstanceType)

except IOError:
    print("nodeinfo.json file does not exist!")

except ValueError:
    print("Not in JSON Format!")

except KeyError as k:
    print(k.message + " field name does not exist!")

except TypeError as t:
    if hostGroupName is None:
        print("hostGroup is null!")
    else:
        print("nodeInstanceType is null!")
