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
package org.apache.batchee.cli;

import io.airlift.command.Cli;
import io.airlift.command.Help;
import io.airlift.command.ParseException;
import org.apache.batchee.cli.command.Restart;
import org.apache.batchee.cli.command.Start;

public class BatchEECLI {
    public static void main(final String[] args) {
        final Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("batchee")
                .withDescription("the stupid content tracker")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, Start.class, Restart.class);

        final Cli<Runnable> parser = builder.build();

        try {
            parser.parse(args).run();
        } catch (final ParseException e) {
            parser.parse("help").run();
        }
    }

    private BatchEECLI() {
        // no-op
    }
}
