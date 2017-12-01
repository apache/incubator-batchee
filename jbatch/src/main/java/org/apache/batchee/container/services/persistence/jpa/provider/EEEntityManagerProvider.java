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

import org.apache.batchee.container.services.factory.CDIBatchArtifactFactory;
import org.apache.batchee.container.services.persistence.jpa.EntityManagerProvider;

import javax.persistence.EntityManager;
import java.util.Properties;

/**
 * Designed to use a container entity manager relying on JTA to commit.
 * The EntityManager is a CDI bean with the qualifier @Named.
 * Default name is "batcheeJpaEm" but it can be customized using "persistence.jpa.ee.entity-manager.name" property.
 *
 * Note: the bean should be scoped @ApplicationScoped.
 *
 * Typically:
 *
 * <pre><code>
 * &#64;PersistenceContext
 * &#64;Produces
 * &#64;Named
 * private EntityManager batcheeJpaEm;
 * </code></pre>
 */
public class EEEntityManagerProvider implements EntityManagerProvider {
    private EntityManager instance;

    @Override
    public EntityManager newEntityManager() {
        return instance;
    }

    @Override
    public void release(final EntityManager entityManager) {
        // no-op
    }

    @Override
    public void init(final Properties batchConfig) {
        final String beanName = batchConfig.getProperty("persistence.jpa.ee.entity-manager.name", "batcheeJpaEm");
        instance = EntityManager.class.cast(new CDIBatchArtifactFactory().load(beanName).getValue());
    }
}
