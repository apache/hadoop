<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->


# Authenticating with Azure

## Before you begin: things to read

* [OAuth 2.0 and OpenID Connect protocols on the Microsoft identity platform](https://docs.microsoft.com/en-us/azure/active-directory/develop/active-directory-v2-protocols).
    Explains the conceptual model, including the structure of endpoint URLs.
* [Microsoft identity platform and OAuth 2.0 authorization code flow](https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow).
    How the ABFS client authenticates with the store.


## Authenticating with OAuth


### Creating an Azure Service Principal

##### Generating the Service Principal

*Warning* the Azure Portal UI moves entries around. The current instructions
date from Feburary 2020.

The key concepts are: you have to register an application, and use the oauth
id/secret as your client keys, and grant the application access to the ADLS
stores you are using. Every account has its own OAuth2 endpoint, which must be
used to authenticate the client.

Note also that the client SDK isn't great at validating auth failures -because
the Azure OAuth endpoints do not return a 40x unauthed error, they return 200
and an HTML page telling the user that they are not authenticated. This makes
troubleshooting connectivity problems harder than need be. Tip: try using
[Cloudstore](https://github.com/steveloughran/cloudstore) to troubleshoot
problems here.

1. Go to [the Azure portal](https://portal.azure.com)
1. Under services in left navigation list, look for _Azure Active Directory_ and
   click it.
1. In _App Registrations_ select "New Registration".
1. Register a new application
1. Remember the _application name_ you create here - that is what you will add
   to your ADL account as authorized user.
1. Go through the wizard to register a new app. For security reasons, restrict
   it to your organization.
1. Once the application is created you will be on its overview page.
1. Copy _Application (client) ID_ -this UUID is the value
   of `fs.adl.oauth2.client.id`.
1. Go to "Credentials and Secrets"
1. Select a key duration and hit save. Note the generated key. This is the value
   of `fs.adl.oauth2.credential`. Tip: save the key expiry date to your calendar
   to remind you to refresh it.
1. Go back to the _App Registrations_ page, and select _Endpoints_
   at the top.
1. Note down the "OAuth 2.0 token endpoint (v2)" URL. This is the value
   of `fs.adl.oauth2.refresh.url`.


Mapping of values to configuration options


| attribute | example | `adls://` key | `abfs://` key |
|-----------|---------|---------------|---------------|
| Application (client) ID | `81d983a6-4c45-4323-a628-04cd7957271` | `fs.adl.oauth2.client.id` | `fs.azure.account.oauth2.client.id` |
| Credential | `ml(N@OPQ=.+^RS1+Xc+ABcd/eFgH2Yz3` | `fs.adl.oauth2.credential` | `?` |
| "OAuth 2.0 token endpoint (v2) | `https://login.microsoftonline.com/719bbd25-193c-45ab-a3cc-8af651718ed8/oauth2/v2.0/token<` | `fs.adl.oauth2.refresh.url` | `` |
|  | `` | `` | `` |
|  | `` | `` | `` |


