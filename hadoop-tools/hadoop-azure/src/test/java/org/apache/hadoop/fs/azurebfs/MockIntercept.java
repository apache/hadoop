package org.apache.hadoop.fs.azurebfs;

import org.mockito.invocation.InvocationOnMock;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;

public interface MockIntercept<T> {
    public void answer(T mockedObj, InvocationOnMock answer) throws AbfsRestOperationException;
}
