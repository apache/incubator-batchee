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
package org.apache.batchee.container.services.persistence.jdbc.database;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

public class DerbyDatabase implements Database {
    private final Collection<String> forbiddenFields = Arrays.asList("read", "write", "commit", "rollback");

    @Override
    public String integer() {
        return "integer";
    }

    @Override
    public String bigint() {
        return "bigint";
    }

    @Override
    public String varchar255() {
        return "varchar(255)";
    }

    @Override
    public String varchar20() {
        return "varchar(20)";
    }

    @Override
    public String blob() {
        return "blob";
    }

    @Override
    public String timestamp() {
        return "timestamp";
    }

    @Override
    public String autoIncrementId() {
        return "NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
    }

    @Override
    public String columnName(final String name) {
        if (forbiddenFields.contains(name)) {
            return name.toUpperCase(Locale.ENGLISH) + "0"; // like OpenJPA?
        }
        return name;
    }
}
