/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.persistence;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.inject.Provider;

/**
 * A {@link MethodInterceptor} that implements nested transactions.
 * <p>
 * Only the outermost transactional method will <code>commit()</code> or
 * <code>rollback()</code> the contextual transaction. This can be verified
 * through {@link TransactionStatus#isNewTransaction()}, which returns
 * <code>true</code> only for the outermost transactional method call.
 * <p>
 */
public class TransactionalMethodInterceptor implements MethodInterceptor {

  private Provider<DataSourceTransactionManager> transactionManagerProvider;

  TransactionalMethodInterceptor(
      Provider<DataSourceTransactionManager> transactionManagerProvider) {
    this.transactionManagerProvider = transactionManagerProvider;
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    DataSourceTransactionManager transactionManager =
        transactionManagerProvider.get();

    DefaultTransactionDefinition transactionDefinition =
        new DefaultTransactionDefinition();
    TransactionStatus transaction = transactionManager.getTransaction(
        transactionDefinition);

    try {
      Object result = invocation.proceed();

      try {
        if (transaction.isNewTransaction()) {
          transactionManager.commit(transaction);
        }
      } catch (UnexpectedRollbackException ignore) {
      }

      return result;
    } catch (Exception e) {
      if (transaction.isNewTransaction()) {
        transactionManager.rollback(transaction);
      }

      throw e;
    }
  }
}
