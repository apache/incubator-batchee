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
import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@Documentation("Read JPA entities from a query.")
public class JpaItemReader extends EntityManagerLocator implements ItemReader {
    @Inject
    @BatchProperty
    @Documentation("The parameter provider ref or qualified name")
    private String parameterProvider;

    @Inject
    @BatchProperty
    @Documentation("The named query to execute")
    private String namedQuery;

    @Inject
    @BatchProperty
    @Documentation("The query to execute if the named query is empty")
    private String query;

    @Inject
    @BatchProperty
    @Documentation("Pagination size")
    private String pageSize;

    @Inject
    @BatchProperty
    @Documentation("Should entities be detached (default false)")
    private String detachEntities;

    @Inject
    @BatchProperty
    @Documentation("Should JPA transaction be used (default false)")
    private String jpaTransaction;

    private int page = 10;
    private int firstResult = 0;
    private BeanLocator.LocatorInstance<EntityManagerProvider> emProvider;
    private BeanLocator.LocatorInstance<ParameterProvider> paramProvider = null;
    private LinkedList<Object> items = new LinkedList<Object>();
    private boolean detach;
    private boolean transaction;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final BeanLocator beanLocator = BeanLocator.Finder.get(locator);

        emProvider = findEntityManager();
        if (parameterProvider != null) {
            paramProvider = beanLocator.newInstance(ParameterProvider.class, parameterProvider);
        }
        if (pageSize != null) {
            page = Integer.parseInt(pageSize, page);
        }
        if (namedQuery == null && query == null) {
            throw new BatchRuntimeException("a query should be provided");
        }
        detach = Boolean.parseBoolean(detachEntities);
        transaction = Boolean.parseBoolean(jpaTransaction);
    }

    @Override
    public void close() throws Exception {
        if (emProvider != null) {
            emProvider.release();
        }
        if (paramProvider != null) {
            paramProvider.release();
        }
    }

    @Override
    public Object readItem() throws Exception {
        if (items.isEmpty()) {
            final Collection<?> objects = nextPage();
            if (objects == null || objects.isEmpty()) {
                return null;
            }

            items.addAll(objects);
            firstResult += items.size();
        }
        return items.pop();
    }

    private Collection<?> nextPage() {
        final EntityManager em = emProvider.getValue().newEntityManager();
        if (transaction) {
            em.getTransaction().begin();
        }
        final Query jpaQuery;
        try {
            if (namedQuery != null) {
                 jpaQuery = em.createNamedQuery(namedQuery);
            } else {
                jpaQuery = em.createQuery(query);
            }
            jpaQuery.setFirstResult(firstResult).setMaxResults(page);
            if (paramProvider != null) {
                paramProvider.getValue().setParameters(jpaQuery);
            }
            final List<?> resultList = jpaQuery.getResultList();
            if (detach) {
                for (final Object o : resultList) {
                    em.detach(o);
                }
            }
            return resultList;
        } finally {
            if (transaction) {
                em.getTransaction().commit();
            }
            emProvider.getValue().release(em);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
