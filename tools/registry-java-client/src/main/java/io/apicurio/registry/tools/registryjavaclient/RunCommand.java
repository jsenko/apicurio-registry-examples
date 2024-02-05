/*
 * Copyright 2023 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.apicurio.registry.tools.registryjavaclient;

import io.apicurio.registry.rest.client.impl.ErrorHandler;
import io.apicurio.registry.rest.client.impl.RegistryClientImpl;
import io.apicurio.registry.rest.v2.beans.ArtifactReference;
import io.apicurio.rest.client.spi.ApicurioHttpClientFactory;
import picocli.CommandLine.Command;

import java.io.InputStream;
import java.util.List;

@Command(name = "run", version = "0.1", mixinStandardHelpOptions = true)
public class RunCommand implements Runnable {


    public void run() {
        try {

            var anyData = readResource("any.proto");
            var errorData = readResource("error.proto");

            var client = new RegistryClientImpl(ApicurioHttpClientFactory.create("http://localhost:8080/apis/registry/v2", new ErrorHandler()));

            var anyMeta = client.createArtifact("default", "any", anyData, List.of());
            var errorMeta = client.createArtifact("default", "error", errorData, List.of(
                    ArtifactReference.builder().name("google/protobuf/any.proto").groupId("default").artifactId("any").version(anyMeta.getVersion()).build()
            ));

            System.err.println("Success");

        } catch (Exception ex) {
            System.err.println("Failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }


    private InputStream readResource(String name) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    }
}
