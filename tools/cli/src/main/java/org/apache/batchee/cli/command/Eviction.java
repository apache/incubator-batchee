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
package org.apache.batchee.cli.command;

import org.apache.batchee.cli.command.api.Command;
import org.apache.batchee.cli.command.api.Option;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.spi.PersistenceManagerService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Command(name = "evict", description = "remove old data, uses embedded configuration (no JAXRS support yet)")
public class Eviction implements Runnable {
    @Option(name = "until", description = "date until when the eviction will occur (excluded), YYYYMMDD format", required = true)
    private String date;

    @Override
    public void run() {
        try {
            final Date date = new SimpleDateFormat("yyyyMMdd").parse(this.date);
            ServicesManager.find().service(PersistenceManagerService.class).cleanUp(date);
        } catch (final ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
