Microsoft Graph user-to-groups mapping implementation
=====================================================
Microsoft Graph API (https://developer.microsoft.com/en-us/graph) provides an interface to retrieve user's groups from services like Azure Active Directory (https://azure.microsoft.com/en-us/services/active-directory/).
This is an implementation of GroupMappingServiceProvider that allows Hadoop setups to retrieve user-to-group mapping from the Microsoft Graph API.

## How does it work?
It does this by first requesting an OAuth2 token using the credentials of the application registered in Azure.
This application has to have the Directory.Read.All application permission for this to work.

After acquiring the token, it queries the Graph API using this token to retrieve a list of groups of the requested user.

## Setting up Azure Active Directory
One first needs to setup the AAD:
  * Create an application registration in Azure.
  * Client-id is that application's id.
  * Get Directory.Read.All permissions for that application.
  * Save that application's client secret to some Hadoop credential provider.

To setup Hadoop to use this new implementation:

    <property>
      <name>hadoop.security.group.mapping</name>
      <value>org.apache.hadoop.security.msgraph.MicrosoftGraphGroupsMapping</value>
    </property>
    <property>
      <name>hadoop.security.ms-graph.groups.oauth2.url</name>
      <value>https://login.microsoftonline.com/(tenant-id)/oauth2/token</value>
    </property>
    <property>
      <name>hadoop.security.ms-graph.groups.oauth2.client-id</name>
      <value>(client-id)</value>
    </property>
