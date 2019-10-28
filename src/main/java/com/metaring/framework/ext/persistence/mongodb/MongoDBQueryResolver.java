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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.metaring.framework.Tools;
import com.metaring.framework.persistence.OperationResult;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.type.series.TextSeries;
import com.metaring.framework.util.ObjectUtil;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

@SuppressWarnings("unused")
class MongoDBQueryResolver {

    private static final Pattern FUNCTION_PARAMETERS_PATTERN = Pattern.compile("(\\((.*?)(\\[?)\\{(.*?)\\}(( |\\n|\\t)*\\{(.*?)\\})*( |\\n|\\t)*(\\]?)\\))", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern OBJECTID_PATTERN = Pattern.compile("[{]\\s*\\\"\\s*[$]oid\\s*\\\"\\s*:\\s*\\\"\\s*\\w+\\s*\\\"\\s*[}]", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern LIMIT_PATTERN = Pattern.compile(".limit\\((.*?)\\d(.*?)\\)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern SKIP_PATTERN = Pattern.compile(".skip\\((.*?)\\d(.*?)\\)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    private MongoDatabase database;
    private DB db;
    private MongoCollection<Document> collection;
    private String function;
    private String functionTail;
    private DataRepresentation parameters;

    @SuppressWarnings("deprecation")
    protected static final CompletableFuture<DataRepresentation> resolve(String query, MongoClient mongoClient, MongoDatabase defaultDatabase, DB defaultDB, Executor asyncExecutor) {

        final CompletableFuture<DataRepresentation> completableFuture = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            String sql = query;
            MongoDBQueryResolver parsedResult = new MongoDBQueryResolver();

            parsedResult.database = defaultDatabase;
            parsedResult.db = defaultDB;

            if (sql.toLowerCase().startsWith("db.")) {
                sql = sql.substring(3);
            }

            String[] toFirstBraceSplit = sql.substring(0, sql.indexOf("(")).split("[.]");
            String collectionName = toFirstBraceSplit[0];
            if (toFirstBraceSplit.length > 2) {
                parsedResult.database = mongoClient.getDatabase(toFirstBraceSplit[0]);
                parsedResult.db = mongoClient.getDB(toFirstBraceSplit[0]);
                collectionName = toFirstBraceSplit[1];
            }

            if(query.endsWith(";")) {
                try {
                    String q = query;
                    Matcher objectId = OBJECTID_PATTERN.matcher(q);
                    while(objectId.find()) {
                        String group = objectId.group(objectId.groupCount());
                        q = q.replace(group, group.toLowerCase().replace(" ", "").replace("{\"$oid\":", "ObjectId(").replace("}", ")"));
                    }
                    completableFuture.complete(Tools.FACTORY_DATA_REPRESENTATION.fromJson(parsedResult.db.eval("var result=null;" + q + "result;").toString()));
                    return;
                } catch(Exception e) {
                    completableFuture.completeExceptionally(new RuntimeException("Error while Parsing and invoking MongoDB client", e));
                }
            }

            parsedResult.collection = parsedResult.database.getCollection(collectionName);

            parsedResult.function = sql.substring(sql.indexOf(collectionName) + collectionName.length() + 1);
            parsedResult.function = parsedResult.function.substring(0, parsedResult.function.indexOf("("));

            parsedResult.functionTail = "";

            Matcher matcher = FUNCTION_PARAMETERS_PATTERN.matcher(sql.substring(sql.indexOf("(")));

            if (matcher.find()) {
                String matchResult = matcher.group(0);
                if (matchResult.length() > 0) {
                    try {
                        parsedResult.functionTail = sql.substring(sql.indexOf(matchResult) + matchResult.length());
                    }
                    catch (Exception e) {
                    }
                    matchResult = "[" + matchResult.substring(matchResult.startsWith("([") ? 2 : 1, matchResult.length() - (matchResult.endsWith("])") ? 2 : 1)) + "]";
                }
                parsedResult.parameters = Tools.FACTORY_DATA_REPRESENTATION.fromJson(matchResult);
            }

            if(ObjectUtil.isNullOrEmpty(parsedResult.parameters)) {
                parsedResult.parameters = Tools.FACTORY_DATA_REPRESENTATION.fromJson("[]");
            }

            try {
                Method method = MongoDBQueryResolver.class.getDeclaredMethod(parsedResult.function, MongoDBQueryResolver.class, Consumer.class);
                method.setAccessible(true);
                method.invoke(null, parsedResult, ((Consumer<DataRepresentation>) completableFuture::complete));
            } catch (Exception e) {
                completableFuture.completeExceptionally(new RuntimeException("Error while Parsing and invoking MongoDB client", e));
            }
        }, asyncExecutor);
        return completableFuture;
    }

    private static final void find(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {

        if (!info.function.equalsIgnoreCase("find")) {
            throw new UnsupportedOperationException("Wrong 'find' function: " + info.function);
        }

        FindIterable<Document> findIterable = null;
        switch (info.parameters.length()) {
            case 1: {
                findIterable = info.collection.find(toBson(info.parameters.first()));
                break;
            }
            case 2: {
                Bson filter = toBson(info.parameters.first());
                if (info.parameters.hasProperties(1)) {
                    findIterable = info.collection.find(filter).projection(toBson(info.parameters.get(1)));
                }
                break;
            }
            default: {
                findIterable = info.collection.find();
                break;
            }
        }

        Matcher limitMatcher = LIMIT_PATTERN.matcher(info.functionTail);
        if (limitMatcher.find()) {
            String limitMatchResult = limitMatcher.group(0).toLowerCase();
            int limit = Integer.parseInt(limitMatchResult.substring(limitMatchResult.indexOf("(") + 1, limitMatchResult.indexOf(")")).trim());
            findIterable = findIterable.limit(limit);
        }

        Matcher skipMatcher = SKIP_PATTERN.matcher(info.functionTail);
        if (skipMatcher.find()) {
            String skipMatchResult = skipMatcher.group(0).toLowerCase();
            int skip = Integer.parseInt(skipMatchResult.substring(skipMatchResult.indexOf("(") + 1, skipMatchResult.indexOf(")")).trim());
            findIterable = findIterable.skip(skip);
        }

        DataRepresentation result = Tools.FACTORY_DATA_REPRESENTATION.create();
        for (Document document : findIterable) {
            result.add(toDataRepresentation(document));
        }
        callback.accept(result);
    }

    private static final void findAndModify(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {
        DataRepresentation params = info.parameters.first();
        Bson filter = toBson(params.get("query"));
        if(params.hasProperties("update")) {
            findAndUpdate(info, params, filter, callback);
        } else if(params.hasProperties("remove")) {
            findAndRemove(info, params, filter, callback);
        }
    }

    private static final void findAndUpdate(MongoDBQueryResolver info, DataRepresentation params, Bson filter, Consumer<DataRepresentation> callback) {
        Document update = toUpdateDocument(params.get("update"));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE);
        if(params.hasProperty("sort")) {
            options.sort(toBson(params.get("sort")));
        }
        if(params.hasProperty("new")) {
            options.returnDocument(ReturnDocument.AFTER);
        }
        if(params.hasProperty("fields")) {
            options.projection(toBson(params.get("fields")));
        }
        if(params.hasProperty("upsert")) {
            options.upsert(params.getTruth("upsert"));
        }
        if(params.hasProperty("bypassDocumentValidation")) {
            options.bypassDocumentValidation(params.getTruth("bypassDocumentValidation"));
        }
        if(params.hasProperty("maxTimeMS")) {
            options.maxTime(params.getDigit("maxTimeMS"), TimeUnit.MILLISECONDS);
        }
        if(params.hasProperty("collation")) {
            options.collation(toCollation(params.get("collation")));
        }
        callback.accept(toDataRepresentation(info.collection.findOneAndUpdate(filter, update, options)));
    }

    private static final void aggregate(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {
        final List<Bson> aggregation = new ArrayList<>();
        info.parameters.forEach(it -> aggregation.add(toBson(it)));
        callback.accept(toDataRepresentation(info.collection.aggregate(aggregation).first()));
    }

    private static final void findAndRemove(MongoDBQueryResolver info, DataRepresentation params, Bson filter, Consumer<DataRepresentation> callback) {
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        if(params.hasProperty("sort")) {
            options.sort(toBson(params.get("sort")));
        }
        if(params.hasProperty("fields")) {
            options.projection(toBson(params.get("fields")));
        }
        if(params.hasProperty("maxTimeMS")) {
            options.maxTime(params.getDigit("maxTimeMS"), TimeUnit.MILLISECONDS);
        }
        if(params.hasProperty("collation")) {
            options.collation(toCollation(params.get("collation")));
        }
        callback.accept(toDataRepresentation(info.collection.findOneAndDelete(filter, options)));
    }

    private static final void insert(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {
        DataRepresentation firstParameter = info.parameters.first();
        List<Document> list = new ArrayList<Document>();
        if(firstParameter.hasLength()) {
            for(DataRepresentation element : firstParameter) {
                list.add(toDocument(element));
            }
        } else {
            list.add(toDocument(info.parameters.first()));
        }
        info.collection.insertMany(list);
        TextSeries keys = Tools.FACTORY_TEXT_SERIES.create();
        for(Document document : list) {
            keys.add(document.getObjectId("_id").toString());
        }
        callback.accept(OperationResult.create((long) list.size(), keys).toDataRepresentation());
    }

    private static final void update(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {
        Bson filter = toBson(info.parameters.first());
        Bson update = toUpdateBson(info.parameters.get(1));
        boolean many = false;
        UpdateOptions updateOptions = new UpdateOptions();
        if (info.parameters.length() == 3) {
            try {
                many = info.parameters.get(2).getTruth("multi") == true;
            }
            catch(Exception e) {
            }
            try {
                updateOptions.upsert(info.parameters.get(2).getTruth("upsert") == true);
            }
            catch(Exception e) {
            }
        }
        UpdateResult updateResult = null;
        if (many) {
            updateResult = info.collection.updateMany(filter, update, updateOptions);
        }
        else {
            updateResult = info.collection.updateOne(filter, update, updateOptions);
        }
        DataRepresentation result = null;
        if (updateResult != null) {
            TextSeries keys = Tools.FACTORY_TEXT_SERIES.create();
            try {
                keys.add(updateResult.getUpsertedId().asObjectId().getValue().toString());
            } catch(Exception e) {
            }
            result = OperationResult.create((long) updateResult.getModifiedCount(), keys).toDataRepresentation();
        }
        callback.accept(result);
    }

    private static final void save(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {
        throw new UnsupportedOperationException("Save method not supported by Java version of MongoDB");
    }

    private static final void remove(MongoDBQueryResolver info, Consumer<DataRepresentation> callback) {
        boolean many = true;
        try {
            many = (!info.parameters.isEmpty() && info.parameters.length() == 1) || info.parameters.getDigit(1) != 1;
        } catch(Exception e) {
        }
        if(many) {
            try {
                many = !info.parameters.get(1).getTruth("justOne");
            } catch(Exception e) {
            }
        }
        DeleteResult deleteResult = null;
        if (info.parameters.isEmpty()) {
            deleteResult = info.collection.deleteMany(new Document());
        }
        else
            if (many) {
                deleteResult = info.collection.deleteMany(toBson(info.parameters.first()));
            }
            else {
                deleteResult = info.collection.deleteOne(toBson(info.parameters.first()));
            }
        DataRepresentation result = null;
        if (deleteResult != null) {
            result = OperationResult.create(deleteResult.getDeletedCount(), Tools.FACTORY_TEXT_SERIES.create()).toDataRepresentation();
        }
        callback.accept(result);
    }

    private static final Document toDocument(String json) {
        return Document.parse(json);
    }

    private static final Bson toBson(String json) {
        return BsonDocument.parse(json);
    }

    private static final Document toDocument(DataRepresentation dataRepresentation) {
        return toDocument(dataRepresentation.toJson());
    }

    private static final Bson toUpdateBson(DataRepresentation dataRepresentation) {
        return toUpdateBson(dataRepresentation, true);
    }

    private static final Document toUpdateDocument(DataRepresentation dataRepresentation) {
        return (Document) toUpdateBson(dataRepresentation, false);
    }

    private static final Bson toUpdateBson(DataRepresentation dataRepresentation, boolean bson) {
        DataRepresentation update = Tools.FACTORY_DATA_REPRESENTATION.create();
        for(String property : dataRepresentation.getProperties()) {
            DataRepresentation element = dataRepresentation.get(property);
            if(!property.startsWith("$")) {
                update.add("$set", Tools.FACTORY_DATA_REPRESENTATION.create().add(property, element));
            } else {
                update.add(property, element);
            }
        }
        return bson ? toBson(update) : toDocument(update);
    }

    private static final Bson toBson(DataRepresentation dataRepresentation) {
        return toBson(dataRepresentation.toJson());
    }

    private static final Collation toCollation(DataRepresentation dataRepresentation) {
        Collation.Builder builder = Collation.builder();
        if(dataRepresentation.hasProperty("locale")) {
            builder.locale(dataRepresentation.getText("locale"));
        }
        if(dataRepresentation.hasProperty("caseLevel")) {
            builder.caseLevel(dataRepresentation.getTruth("caseLevel"));
        }
        if(dataRepresentation.hasProperty("caseFirst")) {
            builder.collationCaseFirst(CollationCaseFirst.fromString(dataRepresentation.getText("caseFirst")));
        }
        if(dataRepresentation.hasProperty("strength")) {
            builder.collationStrength(CollationStrength.fromInt(dataRepresentation.getDigit("strength").intValue()));
        }
        if(dataRepresentation.hasProperty("numericOrdering")) {
            builder.numericOrdering(dataRepresentation.getTruth("numericOrdering"));
        }
        if(dataRepresentation.hasProperty("alternate")) {
            builder.collationAlternate(CollationAlternate.fromString(dataRepresentation.getText("alternate")));
        }
        if(dataRepresentation.hasProperty("maxVariable")) {
            builder.collationMaxVariable(CollationMaxVariable.fromString(dataRepresentation.getText("maxVariable")));
        }
        if(dataRepresentation.hasProperty("backwards")) {
            builder.backwards(dataRepresentation.getTruth("backwards"));
        }
        return builder.build();

    }

    private static final DataRepresentation toDataRepresentation(Document document) {
        return Tools.FACTORY_DATA_REPRESENTATION.fromJson(document == null ? "null" : document.toJson());
    }
}
