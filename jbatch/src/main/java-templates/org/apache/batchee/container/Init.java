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
package org.apache.batchee.container;

import org.apache.batchee.container.services.ServicesManager;

import java.util.logging.Logger;

public final class Init {
    private final static Logger LOGGER = Logger.getLogger(Init.class.getName());
    private final static String LOGO = "\n" +
        " ____        _       _     ______ ______ \n" +
        "|  _ \\      | |     | |   |  ____|  ____|\n" +
        "| |_) | __ _| |_ ___| |__ | |__  | |__   \n" +
        "|  _ < / _` | __/ __| '_ \\|  __| |  __|  \n" +
        "| |_) | (_| | || (__| | | | |____| |____ \n" +
        "|____/ \\__,_|\\__\\___|_| |_|______|______|${project.version}";

    public static void doInit() {
        if (Boolean.parseBoolean(ServicesManager.value("org.apache.batchee.init.verbose", "true"))) {
            String splashScreen = ServicesManager.value("org.apache.batchee.init.splashscreen", null);
            if (splashScreen == null) {
                splashScreen = LOGO;
            }

            if (!Boolean.parseBoolean(ServicesManager.value("org.apache.batchee.init.verbose.sysout", "false"))) {
                LOGGER.info(splashScreen);
            } else {
                System.out.println(splashScreen);
            }
        }
    }

    private Init() {
        // no-op
    }
}
