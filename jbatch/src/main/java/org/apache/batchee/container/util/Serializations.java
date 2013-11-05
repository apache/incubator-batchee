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
package org.apache.batchee.container.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class Serializations {
    /**
     * This method is used to serialized an object saved into a table BLOB field.
     *
     * @param theObject the object to be serialized
     * @return a object byte array
     * @throws java.io.IOException
     */
    public static byte[] serialize(final Serializable theObject) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(baos);
        oout.writeObject(theObject);
        final byte[] data = baos.toByteArray();
        baos.close();
        oout.close();
        return data;
    }

    /**
     * This method is used to de-serialized a table BLOB field to its original object form.
     *
     * @param buffer the byte array save a BLOB
     * @return the object saved as byte array
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static Serializable deserialize(final byte[] buffer) throws IOException, ClassNotFoundException {
        Serializable theObject = null;
        if (buffer != null) {
            final ObjectInputStream objectIn = new TCCLObjectInputStream(new ByteArrayInputStream(buffer));
            theObject = Serializable.class.cast(objectIn.readObject());
            objectIn.close();
        }
        return theObject;
    }

    private Serializations() {
        // no-op
    }
}
