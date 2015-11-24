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
package org.apache.batchee.csv.mapper;

import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(Theories.class)
public class PrimitivesTest {
    @DataPoints
    public static CoercingConverter[] converters() {
        return new CoercingConverter[]{ new Primitives(), new XBeanConverter() };
    }

    @Theory
    public void defaultValues(final CoercingConverter converter) {
        assertEquals(0L, converter.valueFor(long.class, ""));
        assertEquals((short) 0, converter.valueFor(short.class, ""));
        assertEquals(0, converter.valueFor(int.class, ""));
        assertEquals((byte) 0, converter.valueFor(byte.class, ""));
        assertEquals((char) 0, converter.valueFor(char.class, ""));
        assertEquals(0.f, converter.valueFor(float.class, ""));
        assertEquals(0., converter.valueFor(double.class, ""));
    }

    @Theory
    public void primitives(final CoercingConverter converter) {
        assertEquals(1L, converter.valueFor(long.class, "1"));
        assertEquals((short) 1, converter.valueFor(short.class, "1"));
        assertEquals(1, converter.valueFor(int.class, "1"));
        assertEquals((byte) 1, converter.valueFor(byte.class, "1"));
        assertEquals(1.f, converter.valueFor(float.class, "1"));
        assertEquals(1., converter.valueFor(double.class, "1"));
        assertEquals('1', converter.valueFor(char.class, "1"));
    }
}
