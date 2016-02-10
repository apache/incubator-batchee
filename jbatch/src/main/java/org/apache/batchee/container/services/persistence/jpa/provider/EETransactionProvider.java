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
import org.apache.batchee.container.services.transaction.JTAUserTransactionAdapter;

import javax.persistence.EntityManager;
import javax.transaction.Transaction;
import java.util.Properties;

public class EETransactionProvider implements TransactionProvider {
    private JTAUserTransactionAdapter jta;

    @Override
    public Object start(final EntityManager em) { // ensure internal actions are done in a dedicated tx
        return jta.beginSuspending();
    }

    @Override
    public void commit(final Object o) {
        try {
            jta.commit();
        } finally {
            if (o != null) {
                jta.resume(Transaction.class.cast(o));
            }
        }
    }

    @Override
    public void rollback(final Object tx, final Exception e) {
        jta.rollback(); // TODO: check status or not that important?
    }

    @Override
    public void init(final Properties batchConfig) {
        jta = new JTAUserTransactionAdapter(); // reuse the existing logic to get the tx mgr
    }
}
