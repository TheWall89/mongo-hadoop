package com.mongodb.hadoop.input;

// Mongo

import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoPathRetriever;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

// Hadoop
// Commons

public class MongoAggregateRecordReader extends RecordReader<Object,
        BSONObject> {

    private BSONObject current;
    private final MongoAggregateInputSplit split;
    private final DBCollection coll;
    private final Cursor cursor;
    private float seen = 0;
    private float total;

    private static final Log LOG = LogFactory.getLog(
            MongoAggregateRecordReader.class);


    public MongoAggregateRecordReader(final MongoAggregateInputSplit split) {
        this.split = split;
        coll = split.getColl();
        cursor = split.getCursor();
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
            MongoConfigUtil.close(coll.getDB().getMongo());
        }
    }

    @Override
    public Object getCurrentKey() {
        Object key = MongoPathRetriever.get(current, split.getKeyField());
        return null != key ? key : NullWritable.get();
    }

    @Override
    public BSONObject getCurrentValue() {
        return current;
    }

    public float getProgress() {
        try {
            return cursor.hasNext() ? 0.0f : 1.0f;
        } catch (MongoException e) {
            return 1.0f;
        }
    }

    @Override
    public void initialize(final InputSplit split,
                           final TaskAttemptContext context) {
        total = 1.0f;
    }

    @Override
    public boolean nextKeyValue() {
        try {
            if (!cursor.hasNext()) {
                LOG.info("Read " + seen + " documents from:");
                LOG.info(split.toString());
                return false;
            }

            current = cursor.next();
            seen++;

            return true;
        } catch (MongoException e) {
            LOG.error("Exception reading next key/val from mongo: " +
                      e.getMessage());
            return false;
        }
    }


}
