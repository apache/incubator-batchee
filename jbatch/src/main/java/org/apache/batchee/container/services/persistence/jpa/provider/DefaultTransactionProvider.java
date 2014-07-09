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
package org.apache.batchee.container.services.persistence.jpa.provider;

import org.apache.batchee.container.services.persistence.jpa.TransactionProvider;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import java.util.Properties;

public class DefaultTransactionProvider implements TransactionProvider {
    @Override
    public Object start(final EntityManager em) {
        final EntityTransaction transaction = em.getTransaction();
        transaction.begin();
        return transaction;
    }

    @Override
    public void commit(final Object o) {
        EntityTransaction.class.cast(o).commit();
    }

    @Override
    public void rollback(final Object tx, final Exception e) {

        EntityTransaction.class.cast(tx).rollback();
    }

    @Override
    public void init(final Properties batchConfig) {
        // no-op
    }
}
