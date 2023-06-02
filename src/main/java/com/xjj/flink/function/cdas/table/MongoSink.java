package com.xjj.flink.function.cdas.table;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.xjj.flink.function.cdas.entity.Operator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhoujuncheng
 * @date 2022/6/9
 */
@Slf4j
public class MongoSink extends AbstractCdasSinkFunction<String> {

    MongoClient mongoClient;
    Map<String, MongoCollection<Document>> collections = new HashMap<>();
    Map<String, List<WriteModel<Document>>> collectionData = new HashMap<>();
    MongoDatabase mongoDatabase;

    public MongoSink(String url, String username, String password, String database, int batchSize, long interval) {
        super(url, username, password, database, batchSize, interval);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        createConnection();
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    @Override
    void saveRecords() throws Exception {
        if (collections.size() > 0) {
            for (Map.Entry<String, MongoCollection<Document>> m : collections.entrySet()) {
                MongoCollection<Document> collection = m.getValue();
                List<WriteModel<Document>> datas = collectionData.get(m.getKey());
                if (datas.size() > 0) {
                    collection.bulkWrite(datas);
                }
                datas.clear();
            }
        }
    }


    @Override
    void processData(Operator record) throws Exception {

        MongoCollection<Document> mongoCollection;
        List<WriteModel<Document>> datas;

        if (!collections.containsKey(record.getTablename())) {
            mongoCollection = mongoDatabase.getCollection(record.getTablename());
            collections.put(record.getTablename(), mongoCollection);
            if (record.getPr().size() > 0) {
                //创建索引
                if (record.getPr().size() > 0) {
                    Document document = new Document();
                    for (String key : record.getPr().keySet()) {
                        document.put(key, 1);
                    }
                    mongoCollection.createIndex(document);
                }
            }
            datas = new ArrayList();
            collectionData.put(record.getTablename(), datas);
        } else {
            datas = collectionData.get(record.getTablename());
        }

        Document data = new Document();
        Document content = new Document();
        content.put("dbz_op_type", record.getOpType());
        content.put("dbz_op_time", record.getOpts());
        for (Map.Entry<String, Object> entry : record.getColumnsValue().entrySet()) {
            content.put(entry.getKey(), entry.getValue());
        }


        Document query = new Document();
        if (record.getPr().size() > 0) {
            for (Map.Entry<String, Object> entry : record.getPr().entrySet()) {
                query.put(entry.getKey(), entry.getValue());
            }
        }

        data.put("$set", content);
        UpdateOptions updateOptions = new UpdateOptions().upsert(false);
        UpdateOneModel<Document> updateOneModel = new UpdateOneModel(query, data, updateOptions);
        datas.add(updateOneModel);


    }

    @Override
    void createConnection() throws Exception {
        mongoClient = MongoClients.create(url);
        mongoDatabase = mongoClient.getDatabase(this.database);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {

        if (value == null) {
            return;
        }

        Operator kingbaseOperator = deserialize(value);
        processData(kingbaseOperator);
        elements.add(value.getString(0).toString());

        if (elements.size() >= batchSize) {
            saveRecords();
            elements.clear();
        }
    }
}
