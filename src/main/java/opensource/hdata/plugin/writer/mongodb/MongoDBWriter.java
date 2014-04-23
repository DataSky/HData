package opensource.hdata.plugin.writer.mongodb;

import java.net.UnknownHostException;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.Fields;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;

import org.apache.commons.lang3.ArrayUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDBWriter extends Writer {

    private Fields fields;
    private MongoClient mongoClient = null;
    private DBCollection coll;
    private BasicDBObject[] insertDocs;
    private int batchsize;
    private int count;

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        fields = context.getFields();
        batchsize = writerConfig.getInt(MongoDBWriterProperties.BATCH_INSERT_SIZE, 1000);
        insertDocs = new BasicDBObject[batchsize];
        MongoClientURI clientURI = new MongoClientURI(writerConfig.getString(MongoDBWriterProperties.URI));
        try {
            mongoClient = new MongoClient(clientURI);
            DB db = mongoClient.getDB(clientURI.getDatabase());
            coll = db.getCollection(clientURI.getCollection());
        } catch (UnknownHostException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(Record record) {
        BasicDBObject doc = new BasicDBObject();
        for (int i = 0, len = fields.size(); i < len; i++) {
            doc.put(fields.get(i), record.getField(i));
        }

        insertDocs[count++] = doc;
        if (count == batchsize) {
            coll.insert(insertDocs);
            count = 0;
        }
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            if (count > 0) {
                coll.insert(ArrayUtils.subarray(insertDocs, 0, count));
            }
            mongoClient.close();
        }
    }
}
