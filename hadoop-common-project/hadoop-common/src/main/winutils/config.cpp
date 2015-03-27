/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with this
* work for additional information regarding copyright ownership. The ASF
* licenses this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

#include "winutils.h"
#include <string.h>
#include <stdlib.h>
#import "msxml6.dll" exclude("ISequentialStream", "_FILETIME")

#define ERROR_CHECK_HRESULT_DONE(hr, message)                               \
  if (FAILED(hr))  {                                                        \
    dwError = (DWORD) hr;                                                   \
    LogDebugMessage(L"%s: %x", message, hr);                                \
    goto done;                                                              \
  }

DWORD BuildPathRelativeToModule(
    __in LPCWSTR relativePath, 
    __in size_t len, 
    __out_ecount(len) LPWSTR buffer) {
  DWORD dwError = ERROR_SUCCESS;
  WCHAR moduleFile[MAX_PATH];
  WCHAR modulePath[_MAX_DIR];
  WCHAR moduleDrive[_MAX_DRIVE];
  DWORD size;
  HRESULT hr = S_OK;
  errno_t errno;

  size = GetModuleFileName(NULL, moduleFile, MAX_PATH);
  dwError = GetLastError(); // Always check due to ERROR_INSUFFICIENT_BUFFER
  if (dwError) {
     LogDebugMessage(L"GetModuleFileName: %x\n", dwError);
     goto done;
  }

  errno = _wsplitpath_s(moduleFile,
     moduleDrive, _MAX_DRIVE,
     modulePath, _MAX_DIR,
     NULL, 0,  // fname, not interesting
     NULL, 0); // extenssion, not interesting
  if (errno) {
    LogDebugMessage(L"_wsplitpath_s: %x\n", errno);
    dwError = ERROR_BAD_PATHNAME;
    goto done;
  }

  hr = StringCbPrintf(buffer, len, L"%s%s%s", moduleDrive, modulePath, relativePath);
  if (FAILED(hr)) {
    // There is no reliable HRESULT to WIN32 mapping, use code. 
    // see http://blogs.msdn.com/b/oldnewthing/archive/2006/11/03/942851.aspx
    //
    dwError = HRESULT_CODE(hr);
    goto done;
  }

  LogDebugMessage(L"BuildPathRelativeToModule: %s (%s)\n", buffer, relativePath);

done:
  return dwError;
}

DWORD GetConfigValue(
  __in LPCWSTR relativePath,
  __in LPCWSTR keyName, 
  __out size_t* len, __out_ecount(len) LPCWSTR* value) {

  DWORD dwError = ERROR_SUCCESS;
  WCHAR xmlPath[MAX_PATH];

  *len = 0;
  *value = NULL;

  dwError = BuildPathRelativeToModule(
    relativePath,
    sizeof(xmlPath)/sizeof(WCHAR),
    xmlPath);

  if (dwError) {
    goto done;
  }

  dwError = GetConfigValueFromXmlFile(xmlPath, keyName, len, value);

done:
  if (*len) {
    LogDebugMessage(L"GetConfigValue:%d key:%s len:%d value:%.*s from:%s\n", dwError, keyName, *len, *len, *value, xmlPath);
  }
  return dwError;
}


DWORD GetConfigValueFromXmlFile(__in LPCWSTR xmlFile, __in LPCWSTR keyName, 
  __out size_t* outLen, __out_ecount(len) LPCWSTR* outValue) {

  DWORD dwError = ERROR_SUCCESS;
  HRESULT hr;
  WCHAR keyXsl[8192];
  size_t len = 0;
  LPWSTR value = NULL;
  BOOL comInitialized = FALSE;

  *outLen = 0;
  *outValue = NULL;

  hr = CoInitialize(NULL);
  ERROR_CHECK_HRESULT_DONE(hr, L"CoInitialize");
  comInitialized = TRUE;

  hr = StringCbPrintf(keyXsl, sizeof(keyXsl), L"//configuration/property[name='%s']/value/text()", keyName);
  ERROR_CHECK_HRESULT_DONE(hr, L"StringCbPrintf");

  try {
    MSXML2::IXMLDOMDocument2Ptr pDoc;
    hr = pDoc.CreateInstance(__uuidof(MSXML2::DOMDocument60), NULL, CLSCTX_INPROC_SERVER);
    ERROR_CHECK_HRESULT_DONE(hr, L"CreateInstance");

    pDoc->async = VARIANT_FALSE;
    pDoc->validateOnParse = VARIANT_FALSE;
    pDoc->resolveExternals = VARIANT_FALSE;
    
    _variant_t file(xmlFile);
    
    if (VARIANT_FALSE == pDoc->load(file)) {
      dwError = pDoc->parseError->errorCode;
      LogDebugMessage(L"load %s failed:%d %s\n", xmlFile, dwError, 
        static_cast<LPCWSTR>(pDoc->parseError->Getreason()));
      goto done;
    }

    MSXML2::IXMLDOMElementPtr pRoot = pDoc->documentElement;
    MSXML2::IXMLDOMNodePtr keyNode = pRoot->selectSingleNode(keyXsl);

    if (keyNode) {
      _bstr_t bstrValue = static_cast<_bstr_t>(keyNode->nodeValue);
      len = bstrValue.length();
      value = (LPWSTR) LocalAlloc(LPTR, (len+1) * sizeof(WCHAR));
      LPCWSTR lpwszValue = static_cast<LPCWSTR>(bstrValue);
      memcpy(value, lpwszValue, (len) * sizeof(WCHAR));
      LogDebugMessage(L"key:%s :%.*s [%s]\n", keyName, len, value, lpwszValue);
      *outLen = len;
      *outValue = value;
    }
    else {
      LogDebugMessage(L"node Xpath:%s not found in:%s\n", keyXsl, xmlFile);
    }
  } 
  catch(_com_error errorObject) {
    dwError = errorObject.Error();
    LogDebugMessage(L"catch _com_error:%x %s\n", dwError, errorObject.ErrorMessage());
    goto done;
  }
  
done:
  if (comInitialized) {
    CoUninitialize();
  }
  
  return dwError;
}


