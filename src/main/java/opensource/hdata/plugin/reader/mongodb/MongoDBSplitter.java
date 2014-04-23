package opensource.hdata.plugin.reader.mongodb;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.exception.HDataException;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.util.JSON;

public class MongoDBSplitter extends Splitter {

    private static final String OBJECT_ID_KEY = "_id";
    private static final int HEXADECIMAL = 16;

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String uri = readerConfig.getString(MongoDBReaderProperties.URI);
        int parallelism = readerConfig.getParallelism();

        MongoClientURI clientURI = new MongoClientURI(uri);
        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient(clientURI);
            DB db = mongoClient.getDB(clientURI.getDatabase());
            DBCollection coll = db.getCollection(clientURI.getCollection());

            String maxID = "";
            String minID = "";
            DBObject sort = new BasicDBObject();
            sort.put(OBJECT_ID_KEY, -1);
            DBCursor cursor = coll.find().sort(sort).limit(1);
            while (cursor.hasNext()) {
                maxID = cursor.next().get(OBJECT_ID_KEY).toString();
            }

            sort.put(OBJECT_ID_KEY, 1);
            cursor = coll.find().sort(sort).limit(1);
            while (cursor.hasNext()) {
                minID = cursor.next().get(OBJECT_ID_KEY).toString();
            }

            if (!maxID.isEmpty() && !minID.isEmpty()) {
                BigInteger maxBigInteger = new BigInteger(maxID, HEXADECIMAL);
                BigInteger minBigInteger = new BigInteger(minID, HEXADECIMAL);
                BigInteger step = (maxBigInteger.subtract(minBigInteger).divide(BigInteger.valueOf(parallelism)));
                for (int i = 0, len = parallelism; i < len; i++) {
                    BasicDBObject condition = null;
                    if (readerConfig.containsKey(MongoDBReaderProperties.QUERY)) {
                        condition = (BasicDBObject) JSON.parse(readerConfig.getString(MongoDBReaderProperties.QUERY));
                    } else {
                        condition = new BasicDBObject();
                    }

                    BasicDBObject idRange = new BasicDBObject("$gte", new ObjectId(minBigInteger.add(step.multiply(BigInteger.valueOf(i))).toString(
                            HEXADECIMAL)));
                    if (i == len - 1) {
                        idRange.append("$lte", new ObjectId(maxBigInteger.toString(HEXADECIMAL)));
                    } else {
                        idRange.append("$lt", new ObjectId(minBigInteger.add(step.multiply(BigInteger.valueOf(i + 1))).toString(HEXADECIMAL)));
                    }

                    condition.put(OBJECT_ID_KEY, idRange);

                    PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                    pluginConfig.put(MongoDBReaderProperties.QUERY, condition);
                    list.add(pluginConfig);
                }
            }
        } catch (UnknownHostException e) {
            throw new HDataException(e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

        return list;
    }
}
