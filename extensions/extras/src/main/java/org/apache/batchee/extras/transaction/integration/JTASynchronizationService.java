/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.extras.transaction.integration;

import javax.batch.operations.BatchRuntimeException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.Status;
import javax.transaction.TransactionSynchronizationRegistry;

public class JTASynchronizationService implements SynchronizationService {
    private final TransactionSynchronizationRegistry delegate;

    public JTASynchronizationService(final String jndi) {
        try {
            delegate = TransactionSynchronizationRegistry.class.cast(new InitialContext().lookup(jndi));
        } catch (final NamingException e) {
            throw new BatchRuntimeException(e);
        }
    }

    @Override
    public void registerSynchronization(final Synchronization s) {
        delegate.registerInterposedSynchronization(new SynchronizationAdapter(s));
    }

    @Override
    public boolean hasTransaction() {
        return delegate.getTransactionStatus() == Status.STATUS_ACTIVE;
    }

    @Override
    public void put(final Object key, final Object value) {
        delegate.putResource(key, value);
    }

    @Override
    public Object get(final Object key) {
        return delegate.getResource(key);
    }

    private static class SynchronizationAdapter implements javax.transaction.Synchronization {
        private final Synchronization delegate;

        public SynchronizationAdapter(final Synchronization s) {
            delegate = s;
        }

        @Override
        public void beforeCompletion() {
            delegate.beforeCompletion();
        }

        @Override
        public void afterCompletion(final int status) {
            if (status == Status.STATUS_ROLLEDBACK) {
                delegate.afterRollback();
            } else if (status == Status.STATUS_COMMITTED) {
                delegate.afterCommit();
            } else {
                throw new BatchRuntimeException("Unexpected status " + status);
            }
        }
    }
}
