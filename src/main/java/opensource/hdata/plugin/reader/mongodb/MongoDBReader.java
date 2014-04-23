package opensource.hdata.plugin.reader.mongodb;

import java.net.UnknownHostException;
import java.util.Set;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.DefaultRecord;
import opensource.hdata.core.Fields;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.OutputFieldsDeclarer;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.RecordCollector;
import opensource.hdata.exception.HDataException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDBReader extends Reader {

    private Fields fields;
    private String uri;
    private BasicDBObject condition;
    private static final String OBJECT_ID_KEY = "_id";

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        uri = readerConfig.getString(MongoDBReaderProperties.URI);
        condition = (BasicDBObject) readerConfig.get(MongoDBReaderProperties.QUERY);
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        MongoClientURI clientURI = new MongoClientURI(uri);
        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient(clientURI);
            DB db = mongoClient.getDB(clientURI.getDatabase());
            DBCollection coll = db.getCollection(clientURI.getCollection());
            DBCursor cur = coll.find(condition);
            while (cur.hasNext()) {
                DBObject doc = cur.next();
                Set<String> keys = doc.keySet();
                Record record = new DefaultRecord(keys.size() - 1);
                if (fields == null) {
                    fields = new Fields();
                    for (String key : keys) {
                        fields.add(key);
                    }
                }

                for (String key : keys) {
                    if (!OBJECT_ID_KEY.equals(key)) {
                        record.addField(doc.get(key));
                    }
                }

                recordCollector.send(record);
            }
        } catch (UnknownHostException e) {
            throw new HDataException(e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }
}
