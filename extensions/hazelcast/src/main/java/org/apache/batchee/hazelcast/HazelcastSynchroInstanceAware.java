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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.io.IOException;

public class HazelcastSynchroInstanceAware {
    @Inject
    @BatchProperty
    @Documentation("The hazelcast instance name")
    protected String instanceName;

    @Inject
    @BatchProperty
    @Documentation("The hazelcast xml configuration")
    protected String xmlConfiguration;

    @Inject
    @BatchProperty
    @Documentation("Is the instance local, if false the component will create a client")
    protected String local;

    @Inject
    @BatchProperty
    @Documentation("The lock name to use")
    protected String lockName;

    protected HazelcastInstance instance() throws IOException {
        if (local == null || "true".equalsIgnoreCase(local)) {
            return HazelcastMemberFactory.findOrCreateMember(xmlConfiguration, instanceName());
        }
        return HazelcastClientFactory.newClient(xmlConfiguration);
    }

    protected String instanceName() {
        if (instanceName == null) {
            instanceName = "hazelcast";
        }
        return instanceName;
    }

    protected String lockName() {
        if (lockName == null) {
            lockName = getClass().getSimpleName();
        }
        return lockName;
    }

    protected ILock findLock() throws IOException {
        return instance().getLock(lockName());
    }
}
