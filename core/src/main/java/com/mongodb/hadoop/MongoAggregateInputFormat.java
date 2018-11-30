package com.mongodb.hadoop;

// Mongo

import com.mongodb.hadoop.input.MongoAggregateInputSplit;
import com.mongodb.hadoop.input.MongoAggregateRecordReader;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.MongoSplitterFactory;
import com.mongodb.hadoop.splitter.SplitFailedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.List;

public class MongoAggregateInputFormat extends InputFormat<Object, BSONObject> {

    private static final Log LOG = LogFactory.getLog(MongoInputFormat.class);

    @Override
    public RecordReader<Object, BSONObject> createRecordReader(InputSplit split,
                                                               TaskAttemptContext context) {
        if (!(split instanceof MongoAggregateInputSplit)) {
            throw new IllegalStateException(
                    "Creation of a new RecordReader requires a " +
                    "MongoAggregateInputSplit instance.");
        }

        final MongoAggregateInputSplit mais = (MongoAggregateInputSplit) split;

        return new MongoAggregateRecordReader(mais);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {
        final Configuration conf = context.getConfiguration();
        try {
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(conf);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using " + splitterImpl.toString() +
                          " to calculate splits.");
            }
            return splitterImpl.calculateSplits();
        } catch (SplitFailedException spfe) {
            throw new IOException(spfe);
        }
    }

}
