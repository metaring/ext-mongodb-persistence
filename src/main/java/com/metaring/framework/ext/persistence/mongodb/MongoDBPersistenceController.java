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

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;

import com.metaring.framework.SysKB;
import com.metaring.framework.Tools;
import com.metaring.framework.functionality.FunctionalityTransactionController;
import com.metaring.framework.persistence.OperationResult;
import com.metaring.framework.persistence.PersistenceController;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.type.series.TextSeries;
import com.metaring.framework.util.StringUtil;

public class MongoDBPersistenceController implements PersistenceController {

    private static final CompletableFuture<Void> END = CompletableFuture.completedFuture(null);

    private MongoClient mongoClient;
    private ClientSession mongoSession;
    private MongoDatabase defaultDatabase;
    private DB defaultDB;
    private boolean normalizeIdFields = false;

    @SuppressWarnings("deprecation")
    @Override
    public final CompletableFuture<FunctionalityTransactionController> init(SysKB sysKB, Executor asyncExecutor) {
        final CompletableFuture<FunctionalityTransactionController> response = new CompletableFuture<>();
        MongoDBConnectionProvider.getInstance(sysKB, asyncExecutor).whenCompleteAsync((result, error) -> {
            try {
                if(error != null) {
                    throw error;
                }
                (this.mongoClient = result).listDatabaseNames().first();
                this.mongoSession = null;
                try {
                    this.mongoSession = this.mongoClient.startSession();
                }
                catch(Exception e) {
                }
                DataRepresentation persistenceSettings = sysKB.get("persistence");
                normalizeIdFields = persistenceSettings.hasProperty("normalizeIdFields") && persistenceSettings.getTruth("normalizeIdFields");
                String databaseName = persistenceSettings.getText("database");
                if (!StringUtil.isNullOrEmpty(databaseName)) {
                    defaultDatabase = mongoClient.getDatabase(databaseName);
                    defaultDB = mongoClient.getDB(databaseName);
                }
                response.complete(this);
            }
            catch (Throwable e) {
                response.completeExceptionally(e);
            }
        }, asyncExecutor);
        return response;
    }

    @Override
    public final CompletableFuture<Void> close(Executor asyncExecutor) {
        if(mongoSession != null) {
            if(mongoSession.hasActiveTransaction()) {
                mongoSession.abortTransaction();
            }
            mongoSession.close();
        }
        defaultDatabase = null;
        mongoSession = null;
        mongoClient = null;
        return END;
    }

    @Override
    public final CompletableFuture<Void> initTransaction(Executor asyncExecutor) {
        return mongoSession == null || mongoSession.hasActiveTransaction() ? END : CompletableFuture.runAsync(mongoSession::startTransaction, asyncExecutor);
    }

    @Override
    public final CompletableFuture<Void> commitTransaction(Executor asyncExecutor) {
        return mongoSession == null || !mongoSession.hasActiveTransaction() ? END : CompletableFuture.runAsync(mongoSession::commitTransaction, asyncExecutor);
    }

    @Override
    public final CompletableFuture<Void> rollbackTransaction(Executor asyncExecutor) {
        return mongoSession == null || !mongoSession.hasActiveTransaction() ? END : CompletableFuture.runAsync(mongoSession::abortTransaction, asyncExecutor);
    }

    @Override
    public final boolean isInTransaction() {
        return mongoSession != null && mongoSession.hasActiveTransaction();
    }

    @Override
    public final CompletableFuture<DataRepresentation> query(String sql, Executor asyncExecutor) {
        final CompletableFuture<DataRepresentation> query = new CompletableFuture<>();
        MongoDBQueryResolver.resolve(sql, mongoClient, defaultDatabase, defaultDB, asyncExecutor).whenCompleteAsync((result, error) -> {
            if(error != null) {
                query.completeExceptionally(error);
            } else {
                query.complete(normalizeIdFields ? MongoDBMetaRingUtilities.normalizeIdFields(result) : result);
            }
        }, asyncExecutor);
        return query;
    }

    @Override
    public final CompletableFuture<OperationResult> update(String sql, Executor asyncExecutor) {
        final CompletableFuture<OperationResult> update = new CompletableFuture<>();
        MongoDBQueryResolver.resolve(sql, mongoClient, defaultDatabase, defaultDB, asyncExecutor).whenCompleteAsync((result, error) -> {
            if(error != null) {
                update.completeExceptionally(error);
            } else {
                update.complete(toOperationResult(result, normalizeIdFields));
            }
        }, asyncExecutor);
        return update;
    }

    private static final OperationResult toOperationResult(DataRepresentation result, boolean normalizeIdFields) {
        if(result == null || result.isNull()) {
            return null;
        }
        result = normalizeIdFields ? MongoDBMetaRingUtilities.normalizeIdFields(result) : result;
        if(result.hasProperty("manipulationNumber") || result.hasProperty("keys")) {
            return result.as(OperationResult.class);
        }
        Long manipulationNumber = 0l;
        TextSeries keys = Tools.FACTORY_TEXT_SERIES.create();
        manipulationNumber += result.hasProperty("nInserted") ? result.getDigit("nInserted") : 0;
        manipulationNumber += result.hasProperty("nUpserted") ? result.getDigit("nUpserted") : 0;
        manipulationNumber += result.hasProperty("nModified") ? result.getDigit("nModified") : 0;
        manipulationNumber += result.hasProperty("nRemoved") ? result.getDigit("nRemoved") : 0;
        if(result.hasProperty("id")) {
            keys.add(result.getText("id"));
        }
        return OperationResult.create(manipulationNumber, keys);
    }
}