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
package org.apache.batchee.tools.services.thread;

import org.apache.batchee.container.util.BatchWorkUnit;

import javax.annotation.Resource;
import javax.ejb.Asynchronous;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.transaction.UserTransaction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Small helper class to allow new threads being created via the
 * {@link org.apache.batchee.tools.services.thread.AsyncEjbBatchThreadPoolService}.
 *
 * @see org.apache.batchee.tools.services.thread.AsyncEjbBatchThreadPoolService
 */
@Singleton
@Lock(LockType.READ)
@TransactionManagement(TransactionManagementType.BEAN)
public class ThreadExecutorEjb {

    /**
     * Used for the TransactionService
     */
    @Resource
    private UserTransaction ut;

    private Set<BatchWorkUnit> runningBatchWorkUnits = Collections.synchronizedSet(new HashSet<BatchWorkUnit>());


    private static ThreadLocal<UserTransaction> userTransactions = new ThreadLocal<UserTransaction>();

    @Asynchronous
    public void executeTask(Runnable work, Object config) {
        try {
            userTransactions.set(ut);
            if (work instanceof BatchWorkUnit) {
                runningBatchWorkUnits.add((BatchWorkUnit) work);
            }

            work.run();
        } finally {
            if (work instanceof BatchWorkUnit) {
                runningBatchWorkUnits.remove(work);
            }
            userTransactions.remove();
        }
    }

    public Set<BatchWorkUnit> getRunningBatchWorkUnits() {
        return runningBatchWorkUnits;
    }

    public static UserTransaction getUserTransaction() {
        return userTransactions.get();
    }
}
