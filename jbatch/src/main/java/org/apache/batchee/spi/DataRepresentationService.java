/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.spi;


/**
 * This SPI takes Data Objects from a JBatch artifact and transfers it into something
 * which can be stored in a Blob in the Database (byte[] in Java).
 * The Objects in question can be parameters passed to a job (jobProperties) or
 * Checkpoint objects from an execution.
 * 
 * This service of course also has to implement the other way around. 
 * It needs to be able to also read all the things stored
 * in the database and transfer it back to Java objects.
 * 
 * In the old days we simply used Java Serialisation to get this byte[].
 * But this has an obvious downside when it comes to migration,
 * update scenarios and ease of use.
 */
public interface DataRepresentationService extends BatchService {

    /**
     * Convert the given dataObject into something which can be stored
     * in a BLOB in the database.
     * 
     * @param dataObject the object to store or {@code null} if no data is to be stored
     * @param <T> 
     * @return the internal representation of the dataObject
     */
    <T> byte[] toInternalRepresentation(T dataObject);

    /**
     * Convert the internal representation stored in the {@link PersistenceManagerService}
     * to the Java objects needed by the batch artifacts.
     * @param internalRepresentation the String representation which we store in the databas
     * @param <T>
     * @return the dataObject
     */
    <T> T toJavaRepresentation(byte[] internalRepresentation);
}
