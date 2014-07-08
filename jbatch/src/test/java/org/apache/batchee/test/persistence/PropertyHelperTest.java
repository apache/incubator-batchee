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
package org.apache.batchee.test.persistence;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.apache.batchee.container.services.persistence.jpa.domain.PropertyHelper;

public class PropertyHelperTest {

    @Test
    public void testWriteProperties() {
        Properties props = new Properties();
        props.put("key1", "val1");
        props.put("key2", "val2");
        props.put("key3", "val3");

        String result = PropertyHelper.propertiesToString(props);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("key1=val1"));

        result = PropertyHelper.propertiesToString(null);
        Assert.assertNull(result);

        result = PropertyHelper.propertiesToString(new Properties());
        Assert.assertNull(result);
    }

    @Test
    public void testReadProperties() {
        String val = "key1=val1\nkey2=val2";

        Properties props = PropertyHelper.stringToProperties(val);
        Assert.assertNotNull(props);
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("val1", props.getProperty("key1"));
        Assert.assertEquals("val2", props.getProperty("key2"));

        props = PropertyHelper.stringToProperties("");
        Assert.assertNotNull(props);
        Assert.assertEquals(0, props.size());

        props = PropertyHelper.stringToProperties(null);
        Assert.assertNotNull(props);
        Assert.assertEquals(0, props.size());
    }
}
