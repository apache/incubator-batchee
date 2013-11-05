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

import org.apache.batchee.extras.util.MyProvider;
import org.apache.batchee.extras.util.Person;
import org.apache.batchee.util.Batches;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class JpaWriterTest {
    @Test
    public void write() throws Exception {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("jpa-writer", new Properties()));

        final Connection c = DriverManager.getConnection("jdbc:derby:memory:jpa;create=true", "app", "app");
        final PreparedStatement select = c.prepareStatement("select name from Person");
        final ResultSet set = select.executeQuery();
        final Collection<String> names = new ArrayList<String>();
        while (set.next()) {
            names.add(set.getString("name"));
        }
        c.close();

        assertEquals(2, names.size());
    }

    @AfterClass
    public static void clear() {
        new MyProvider().cleanup();
    }

    public static class TwoItemsReader implements ItemReader {
        private int count = 0;

        @Override
        public void open(Serializable checkpoint) throws Exception {
            // no-op
        }

        @Override
        public void close() throws Exception {
            // no-op
        }

        @Override
        public Object readItem() throws Exception {
            if (count++ < 2) {
                final Person p = new Person();
                p.setName("line " + count);
                return p;
            }
            return null;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }
}
