/*
 * Copyright 2012 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.its.transaction;

import javax.batch.runtime.context.StepContext;
import javax.transaction.RollbackException;
import javax.transaction.Status;

import java.util.Properties;

import org.apache.batchee.container.exception.TransactionManagementException;
import org.apache.batchee.spi.TransactionManagementService;
import org.apache.batchee.spi.TransactionManagerAdapter;

/**
 * A batchee
 */
public class MockTransactionManagerService implements TransactionManagementService {

    public static MockTransactionManagerAdapter mockTxAdapter = new MockTransactionManagerAdapter();

    @Override
    public TransactionManagerAdapter getTransactionManager(StepContext stepContext)
    {
        return mockTxAdapter;
    }

    @Override
    public void init(Properties batchConfig)
    {

    }

    public static class MockTransactionManagerAdapter implements TransactionManagerAdapter
    {
        private int txStatus = Status.STATUS_NO_TRANSACTION;

        public void setTxStatus(int txStatus)
        {
            this.txStatus = txStatus;
        }

        @Override
        public void begin()
        {
            txStatus = Status.STATUS_ACTIVE;
        }

        @Override
        public void commit()
        {
            switch (txStatus) {
                case Status.STATUS_ACTIVE:
                    txStatus = Status.STATUS_COMMITTED;
                    break;
                case Status.STATUS_ROLLEDBACK:
                case Status.STATUS_MARKED_ROLLBACK:
                    throw new TransactionManagementException(new RollbackException());
                default :
                    throw new TransactionManagementException(new IllegalStateException("no mock-tx on current thread"));
            }
        }

        @Override
        public int getStatus()
        {
            return txStatus;
        }

        @Override
        public void rollback()
        {
            switch (txStatus) {
                case Status.STATUS_ACTIVE:
                    txStatus = Status.STATUS_ROLLEDBACK;
                    break;
                case Status.STATUS_ROLLEDBACK:
                case Status.STATUS_MARKED_ROLLBACK:
                    break;
                default :
                    throw new TransactionManagementException(new IllegalStateException("no mock-tx on current thread"));
            }
        }

        @Override
        public void setRollbackOnly()
        {
            switch (txStatus) {
                case Status.STATUS_ACTIVE:
                case Status.STATUS_MARKED_ROLLBACK:
                    txStatus = Status.STATUS_MARKED_ROLLBACK;
                    break;
                case Status.STATUS_ROLLEDBACK:
                    break;
                default :
                    throw new TransactionManagementException(new IllegalStateException("no mock-tx on current thread"));
            }
        }

        @Override
        public void setTransactionTimeout(int arg0)
        {
            // no op
        }
    }
}
