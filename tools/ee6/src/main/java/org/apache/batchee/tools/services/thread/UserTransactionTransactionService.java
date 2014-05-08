/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.tools.services.thread;

import java.util.Properties;

import javax.batch.runtime.context.StepContext;
import javax.transaction.UserTransaction;

import org.apache.batchee.container.util.ExceptionUtil;
import org.apache.batchee.spi.TransactionManagementService;
import org.apache.batchee.spi.TransactionManagerAdapter;

/**
 * @author <a href="mailto:struberg@yahoo.de">Mark Struberg</a>
 */
public class UserTransactionTransactionService implements TransactionManagementService {
    @Override
    public void init(Properties batchConfig) {

    }

    @Override
    public TransactionManagerAdapter getTransactionManager(StepContext stepContext) {
        UserTransaction ut = ThreadExecutorEjb.getUserTransaction();

        return new UserTransactionTxAdapter(ut);
    }

    private static class UserTransactionTxAdapter implements TransactionManagerAdapter {
        private final UserTransaction ut;

        private UserTransactionTxAdapter(UserTransaction ut) {
            this.ut = ut;
        }

        @Override
        public void begin() {
            try {
                ut.begin();
            } catch (Exception e) {
                ExceptionUtil.throwAsRuntimeException(e);
            }
        }

        @Override
        public void commit() {
            try {
                ut.commit();
            } catch (Exception e) {
                ExceptionUtil.throwAsRuntimeException(e);
            }
        }

        @Override
        public int getStatus() {
            try {
                return ut.getStatus();
            } catch (Exception e) {
                throw ExceptionUtil.throwAsRuntimeException(e);
            }
        }

        @Override
        public void rollback() {
            try {
                ut.rollback();
            } catch (Exception e) {
                ExceptionUtil.throwAsRuntimeException(e);
            }
        }

        @Override
        public void setRollbackOnly() {
            try {
                ut.setRollbackOnly();
            } catch (Exception e) {
                ExceptionUtil.throwAsRuntimeException(e);
            }
        }

        @Override
        public void setTransactionTimeout(int timeout) {
            try {
                ut.setTransactionTimeout(timeout);
            } catch (Exception e) {
                ExceptionUtil.throwAsRuntimeException(e);
            }
        }
    }
}
