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
package org.apache.batchee.hazelcast;

import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.batch.api.Batchlet;
import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

@Documentation("Obtain a hazelcast lock")
public class HazelcastLockBatchlet extends HazelcastSynchroInstanceAware implements Batchlet {
    @Inject
    @BatchProperty
    @Documentation("The duration this task can wait to get the lock")
    protected String tryDuration;

    @Inject
    @BatchProperty
    @Documentation("The duration unit associated to tryDuration")
    protected String tryDurationUnit;

    @Override
    public String process() throws Exception {
        if (tryDuration != null) {
            findLock().lock(Long.parseLong(tryDuration), TimeUnit.valueOf(tryDurationUnit()));
        }
        findLock().lock();
        return "locked";
    }

    protected String tryDurationUnit() {
        if (tryDurationUnit == null) {
            tryDurationUnit = TimeUnit.SECONDS.name();
        }
        return tryDurationUnit;
    }

    @Override
    public void stop() throws Exception {
        // no-op
    }
}
