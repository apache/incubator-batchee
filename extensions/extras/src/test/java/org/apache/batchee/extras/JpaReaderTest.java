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
package org.apache.batchee.extras;

import org.apache.batchee.extras.typed.TypedItemProcessor;
import org.apache.batchee.extras.util.MyProvider;
import org.apache.batchee.extras.util.Person;
import org.apache.batchee.util.Batches;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JpaReaderTest {
    private static MyProvider provider = new MyProvider();

    @BeforeClass
    public static void init() {
        final EntityManager em = provider.newEntityManager();
        em.getTransaction().begin();
        for (int i = 0; i < 12; i++) {
            final Person person = new Person();
            person.setName("toto" + i);
            em.persist(person);
        }
        em.getTransaction().commit();
        provider.release(em);
    }

    @AfterClass
    public static void clear() {
        provider.cleanup();
    }

    @Test
    public void read() throws Exception {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("jpa-reader", new Properties()));
        assertEquals(StoreItems.ITEMS.size(), 12);
        for (int i = 0; i < 12; i++) {
            assertTrue(StoreItems.ITEMS.contains("toto" + i));
        }
    }

    public static class StoreItems extends TypedItemProcessor<Person, Person> {
        public static final Collection<String> ITEMS = new ArrayList<String>();

        @Override
        protected Person doProcessItem(Person item) {
            ITEMS.add(item.getName());
            return item;
        }
    }
}
