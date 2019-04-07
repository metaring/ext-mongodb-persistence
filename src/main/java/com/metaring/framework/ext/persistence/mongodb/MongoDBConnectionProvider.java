/**
 *    Copyright 2019 MetaRing s.r.l.
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.metaring.framework.ext.persistence.mongodb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import com.metaring.framework.SysKB;
import com.metaring.framework.type.DataRepresentation;

final class MongoDBConnectionProvider {

    private static CompletableFuture<MongoClient> INSTANCE;

    static final CompletableFuture<MongoClient> getInstance(SysKB sysKB, Executor asyncExecutor) {
        if (INSTANCE == null) {
            INSTANCE = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                try {
                    DataRepresentation persistenceSettings = sysKB.get("persistence");
                    String username = persistenceSettings.getText("username");
                    MongoCredential mongoCredential = null;
                    if (username != null && username.trim().isEmpty()) {
                        mongoCredential = MongoCredential.createCredential(username, persistenceSettings.getText("database"), persistenceSettings.getText("password").toCharArray());
                    }
                    if (persistenceSettings.hasProperty("url")) {
                        INSTANCE.complete(new MongoClient(new MongoClientURI(persistenceSettings.getText("url"))));
                    }
                    else {
                        int port = 27017;
                        if (persistenceSettings.hasProperty("port")) {
                            port = persistenceSettings.getDigit("port").intValue();
                        }
                        ServerAddress serverAddress = new ServerAddress(persistenceSettings.getText("host"), port);
                        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
                        if (persistenceSettings.hasProperty("enable_ssl")) {
                            optionsBuilder.sslEnabled(persistenceSettings.getTruth("enable_ssl"));
                        }
                        MongoClient mongoClient = mongoCredential == null ? new MongoClient(serverAddress, optionsBuilder.build()) : new MongoClient(serverAddress, mongoCredential, optionsBuilder.build());
                        INSTANCE.complete(mongoClient);
                    }
                }
                catch (Exception e) {
                    INSTANCE.completeExceptionally(e);
                }
            }, asyncExecutor);
        }
        return INSTANCE;
    }
}