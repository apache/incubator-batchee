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

import java.io.Serializable;
import java.util.List;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class LooseModelMapperProcessorTest {
    @Test
    public void map() {
        final JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("modelmapper-processor-loose", null));
        assertNotNull(Writer.items);
        assertEquals(2, Writer.items.size());
        assertTrue(Writer.items.contains(new B("1", new Nested("#1"))));
        assertTrue(Writer.items.contains(new B("2", new Nested("#2"))));
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
        private Nested nested;

        public B(final String s, final Nested s1) {
            this.a = s;
            this.nested = s1;
        }

        public B() {
            // no-op
        }

        public void setA(final String a) {
            this.a = a;
        }

        public String getA() {
            return a;
        }

        public Nested getNested() {
            return nested;
        }

        public void setNested(final Nested nested) {
            this.nested = nested;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            B b1 = (B) o;

            return !(a != null ? !a.equals(b1.a) : b1.a != null) && !(nested != null ? !nested.equals(b1.nested) : b1.nested != null);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (nested != null ? nested.hashCode() : 0);
            return result;
        }
    }

    public static class Nested {
        private String b;

        public Nested() {
            // no-op
        }

        public Nested(final String b) {
            this.b = b;
        }

        public void setB(final String b) {
            this.b = b;
        }

        public String getB() {
            return b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Nested nested = (Nested) o;

            return !(b != null ? !b.equals(nested.b) : nested.b != null);
        }

        @Override
        public int hashCode() {
            return b != null ? b.hashCode() : 0;
        }
    }
}
