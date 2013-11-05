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

import org.apache.batchee.container.services.persistence.jpa.EntityManagerProvider;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Properties;

public class DefaultEntityManagerProvider implements EntityManagerProvider {
    private static final String PERSISTENCE_JPA_PREFIX = "persistence.jpa.property.";

    private EntityManagerFactory emf;
    private Properties config;

    @Override
    public EntityManager newEntityManager() {
        return emf.createEntityManager(config);
    }

    @Override
    public void release(final EntityManager entityManager) {
        entityManager.close();
    }

    @Override
    public void init(final Properties batchConfig) {
        emf = Persistence.createEntityManagerFactory(batchConfig.getProperty("persistence.jpa.unit-name", "batchee"));

        config = new Properties();
        for (final String key : batchConfig.stringPropertyNames()) {
            if (key.startsWith(PERSISTENCE_JPA_PREFIX)) {
                final String realKey = key.substring(PERSISTENCE_JPA_PREFIX.length());
                config.setProperty(realKey, batchConfig.getProperty(key));
            }
        }
    }
}
