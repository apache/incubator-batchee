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

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.exception.TransactionManagementException;
import org.apache.batchee.spi.TransactionManagementService;
import org.apache.batchee.spi.TransactionManagerAdapter;

import javax.batch.runtime.context.StepContext;
import java.util.Properties;

public class DefaultBatchTransactionService implements TransactionManagementService {
    protected Properties batchConfig = null;

    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        this.batchConfig = batchConfig;
    }

    protected TransactionManagerAdapter getTransactionManager() {
        try {
            DefaultBatchTransactionService.class.getClassLoader().loadClass("javax.transaction.UserTransaction");
            return new JTAUserTransactionAdapter();
        } catch (final Throwable ncdfe) {
            return new DefaultNonTransactionalManager();
        }
    }

    @Override
    public TransactionManagerAdapter getTransactionManager(final StepContext stepContext) throws TransactionManagementException {
        // Doesn't currently make use of stepContext but keeping signature
        return  getTransactionManager();
    }

    @Override
    public String toString() {
        return getClass().getName();
    }
}
