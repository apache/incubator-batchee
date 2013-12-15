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
package org.apache.batchee.modelmapper;

import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ModelMapperProcessorTest {
    @Test
    public void map() {
        final JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("modelmapper-processor", null));
        assertNotNull(Writer.items);
        assertEquals(2, Writer.items.size());
        assertTrue(Writer.items.contains(new B("1", "#1")));
        assertTrue(Writer.items.contains(new B("2", "#2")));
    }

    public static class Reader implements ItemReader {
        private int count = 0;

        @Override
        public void open(final Serializable serializable) throws Exception {
            // no-op
        }

        @Override
        public void close() throws Exception {
            // no-op
        }

        @Override
        public Object readItem() throws Exception {
            if (count++ < 2) {
                return new A("" + count, "#" + count);
            }
            return null;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }

    public static class Writer implements ItemWriter {
        private static List<B> items;

        @Override
        public void open(final Serializable serializable) throws Exception {
            // no-op
        }

        @Override
        public void close() throws Exception {
            // no-op
        }

        @Override
        public void writeItems(final List objects) throws Exception {
            items = objects;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }

    public static class A {
        private String a;
        private String b;

        public A(final String a, final String b) {
            this.a = a;
            this.b = b;
        }

        public String getA() {
            return a;
        }

        public String getB() {
            return b;
        }
    }

    public static class B {
        private String a;
        private String b;

        public B(final String s, final String s1) {
            this.a = s;
            this.b = s1;
        }

        public B() {
            // no-op
        }

        public void setA(final String a) {
            this.a = a;
        }

        public void setB(final String b) {
            this.b = b;
        }

        public String getA() {
            return a;
        }

        public String getB() {
            return b;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final B b1 = (B) o;
            return a.equals(b1.a) && b.equals(b1.b);
        }

        @Override
        public int hashCode() {
            int result = a.hashCode();
            result = 31 * result + b.hashCode();
            return result;
        }
    }
}
