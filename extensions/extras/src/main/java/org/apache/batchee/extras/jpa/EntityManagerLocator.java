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
package org.apache.batchee.extras.jpa;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.locator.BeanLocator;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;

public class EntityManagerLocator {
    @Inject
    @BatchProperty
    @Documentation("The reference of qualified name of the proviedr for the entity manager")
    protected String entityManagerProvider;

    @Inject
    @BatchProperty
    @Documentation("The locator to use to find the entity manager provider")
    protected String locator;

    protected BeanLocator.LocatorInstance<EntityManagerProvider> emProvider;

    protected BeanLocator.LocatorInstance<EntityManagerProvider> findEntityManager() {
        if (entityManagerProvider != null && entityManagerProvider.startsWith("jndi:")) {
            return new BeanLocator.LocatorInstance<EntityManagerProvider>(new JndiEntityManagerProvider(entityManagerProvider.substring("jndi:".length())), null);
        }
        return BeanLocator.Finder.get(locator).newInstance(EntityManagerProvider.class, entityManagerProvider);
    }

    private static class JndiEntityManagerProvider implements EntityManagerProvider {
        private final String name;

        public JndiEntityManagerProvider(final String jndi) {
            name = jndi;
        }

        @Override
        public EntityManager newEntityManager() {
            try {
                return EntityManager.class.cast(new InitialContext().lookup(name));
            } catch (final NamingException e) {
                throw new IllegalArgumentException("Wrong name: " + name);
            }
        }

        @Override
        public void release(final EntityManager em) {
            // no-op: container managed
        }
    }
}
