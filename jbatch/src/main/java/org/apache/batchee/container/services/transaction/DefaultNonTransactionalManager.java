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
package org.apache.batchee.container.services.transaction;

import org.apache.batchee.container.exception.TransactionManagementException;
import org.apache.batchee.spi.TransactionManagerAdapter;

public class DefaultNonTransactionalManager implements TransactionManagerAdapter {
    private int txStatus = 6; // javax.transaction.Status.STATUS_NO_TRANSACTION

    @Override
    public void begin() throws TransactionManagementException {
        txStatus = 0; // javax.transaction.Status.STATUS_ACTIVE
    }

    @Override
    public void commit() throws TransactionManagementException {
        txStatus = 3; // javax.transaction.Status.STATUS_COMMITTED
    }

    @Override
    public void rollback() throws TransactionManagementException {
        txStatus = 4; // javax.transaction.Status.STATUS_ROLLEDBACK
    }

    @Override
    public int getStatus() throws TransactionManagementException {
        return txStatus;
    }

    @Override
    public void setRollbackOnly() throws TransactionManagementException {
        txStatus = 9;  // javax.transaction.Status.STATUS_ROLLING_BACK
    }

    @Override
    public void setTransactionTimeout(int seconds) throws TransactionManagementException {
        // no-op
    }
}
