/*
 * Copyright 2012 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.container.impl.controller.chunk;

import org.junit.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ExceptionConfigTest {
    @Test
    public void directInclude() {
        final ExceptionConfig config = new ExceptionConfig();
        config.getIncludes().add(A.class.getName());
        assertTrue(config.accept(new A()));
    }

    @Test
    public void directExclude() {
        final ExceptionConfig config = new ExceptionConfig();
        config.getExcludes().add(A.class.getName());
        assertFalse(config.accept(new A()));
    }

    @Test
    public void closestClass() {
        final ExceptionConfig config = new ExceptionConfig();
        config.getIncludes().add(C.class.getName());
        config.getIncludes().add(A.class.getName());
        config.getExcludes().add(B.class.getName());
        assertTrue(config.accept(new A()));
        assertTrue(config.accept(new C()));
        assertFalse(config.accept(new B()));
    }

    public static class A extends Exception {}
    public static class B extends A {}
    public static class C extends B {}
}
