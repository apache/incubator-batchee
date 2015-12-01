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

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.io.Serializable;
import java.util.List;

@Documentation("Write all items using JPA")
public class JpaItemWriter  extends EntityManagerLocator implements ItemWriter {
    @Inject
    @BatchProperty
    @Documentation("If set to true merge() will be used instead of persist()")
    private String useMerge;

    @Inject
    @BatchProperty
    @Documentation("Should JPA transaction be used (default false)")
    private String jpaTransaction;

    private boolean merge;
    private boolean transaction;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        emProvider = findEntityManager();
        merge = Boolean.parseBoolean(useMerge);
        transaction = Boolean.parseBoolean(jpaTransaction);
    }

    @Override
    public void close() throws Exception {
        if (emProvider != null) {
            emProvider.release();
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        final EntityManager em = emProvider.getValue().newEntityManager();
        try {
            if (transaction) {
                em.getTransaction().begin();
            }

            for (final Object o : items) {
                if (!merge) {
                    em.persist(o);
                } else {
                    em.merge(o);
                }
            }

            if (transaction) {
                em.getTransaction().commit();
            } else {
                em.flush();
            }
        } finally {
            emProvider.getValue().release(em);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
