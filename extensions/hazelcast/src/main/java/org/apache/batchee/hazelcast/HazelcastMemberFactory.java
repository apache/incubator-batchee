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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;

final class HazelcastMemberFactory {
    public static HazelcastInstance findOrCreateMember(final String xmlConfiguration, String instanceName) throws IOException {
        final HazelcastInstance found = Hazelcast.getHazelcastInstanceByName(instanceName);
        if (found != null) {
            return found;
        }

        final Config config;
        if (xmlConfiguration != null) {
            config =  new XmlConfigBuilder(IOs.findConfiguration(xmlConfiguration)).build();
        } else {
            config =  new XmlConfigBuilder().build();
        }
        if (instanceName != null) {
            config.setInstanceName(instanceName);
        }
        return Hazelcast.newHazelcastInstance(config);
    }

    private HazelcastMemberFactory() {
        // no-op
    }
}
