<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# CLI

BatchEE provides a CLI to make it easy to integrate with companies schedulers
and let JBatch batches run as any program.

## BatchEE CLI usage

BatchEE cli has several mode and can be used as a base dependency to build shades
or fatjars but strength of it is revealed when used as a product:

* you extract batchee-cli zip somewhere on your disk (`/opt/apache/batchee/cli` for instance)
* you extract your application somewhere else  (`/opt/company/application/application.war` for instance)
* you run one of the batch of the war with `/opt/apache/batchee/cli/bin/batchee start -lifecycle openejb -archive /opt/company/application/application.war -name mybatch`

Tip: `batchee` script help can be accessed calling it without any argument. To get a detail help on a particular command just prefix its name byu `help`, for instance to
get help on `start` command launch `./bin/batchee help start`.

As you can see with last bullet, BatchEE CLI provides several options to start a batch. Here few interesting ones (you can get the full list/description calling `help` on it):

* `-lifecycle`: allows to start/stop a container and run the batch once it is launched. Previous sample was running it in OpenEJB (TomEE) allowing you to rely on CDI, EJB, JTA, JPA...
BatchEE also provides integration for Spring or custom lifecycles/containers.
* `-sharedLibs`: allows to add to the container/BatchEE classloader a set of jars (all the jars in the specified folder). Useful to be able to keep BatchEE, your container and your batches
separated on the disk to allow partial updates (updating BatchEE without updating OpenEJB or the application for instance).
* `-archive`: specify which application contain the batch to run, this can be a `war` or a `bar`. If you want to deploy a jar no need to specify this option, just ensure you added it in `-libs`.
