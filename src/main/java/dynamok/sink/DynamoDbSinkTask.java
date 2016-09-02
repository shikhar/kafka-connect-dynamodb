package dynamok.sink;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import dynamok.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDbSinkTask extends SinkTask {

    private enum ValueSource {
        RECORD_KEY {
            @Override
            String topAttributeName(ConnectorConfig config) {
                return config.topKeyAttribute;
            }
        },
        RECORD_VALUE {
            @Override
            String topAttributeName(ConnectorConfig config) {
                return config.topValueAttribute;
            }
        };

        abstract String topAttributeName(ConnectorConfig config);
    }

    private final Logger log = LoggerFactory.getLogger(DynamoDbSinkTask.class);

    private ConnectorConfig config;
    private AmazonDynamoDBClient client;
    private int remainingRetries;

    @Override
    public void start(Map<String, String> props) {
        config = new ConnectorConfig(props);
        client = new AmazonDynamoDBClient();
        client.configureRegion(config.region);
        remainingRetries = config.maxRetries;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;
        final Map<String, List<WriteRequest>> writesByTable = new HashMap<>();
        for (SinkRecord record : records) {
            final String tableName = config.tableFormat.replace("${topic}", record.topic());
            List<WriteRequest> writes = writesByTable.get(tableName);
            if (writes == null) {
                writes = new ArrayList<>();
                writesByTable.put(tableName, writes);
            }
            final PutRequest put = new PutRequest();
            if (!config.ignoreRecordValue) {
                insert(ValueSource.RECORD_VALUE, record.valueSchema(), record.value(), put);
            }
            if (!config.ignoreRecordKey) {
                insert(ValueSource.RECORD_KEY, record.keySchema(), record.key(), put);
            }
            maybeInsertKafkaCoordinates(record, put);
            writes.add(new WriteRequest(put));
        }
        try {
            final BatchWriteItemResult batchWriteResponse = client.batchWriteItem(new BatchWriteItemRequest(writesByTable));
            if (!batchWriteResponse.getUnprocessedItems().isEmpty()) {
                throw new UnprocessedItemsException(batchWriteResponse.getUnprocessedItems());
            }
        } catch (AmazonDynamoDBException | UnprocessedItemsException e) {
            log.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, e);
            if (remainingRetries == 0) {
                throw new ConnectException(e);
            } else {
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(e);
            }
        }
        remainingRetries = config.maxRetries;
    }

    private void insert(ValueSource valueSource, Schema schema, Object value, PutRequest put) {
        final AttributeValue attributeValue =
                schema == null
                        ? AttributeValueConverter.toAttributeValueSchemaless(value)
                        : AttributeValueConverter.toAttributeValue(schema, value);
        final String topAttributeName = valueSource.topAttributeName(config);
        if (!topAttributeName.isEmpty()) {
            put.addItemEntry(topAttributeName, attributeValue);
        } else if (attributeValue.getM() != null) {
            put.setItem(attributeValue.getM());
        } else {
            throw new ConnectException("No top attribute name configured for " + valueSource + ", and it could not be converted to Map: " + attributeValue);
        }
    }

    private void maybeInsertKafkaCoordinates(SinkRecord record, PutRequest put) {
        if (config.kafkaCoordinateNames != null) {
            put.addItemEntry(config.kafkaCoordinateNames.topic, new AttributeValue().withS(record.topic()));
            put.addItemEntry(config.kafkaCoordinateNames.partition, new AttributeValue().withN(String.valueOf(record.kafkaPartition())));
            put.addItemEntry(config.kafkaCoordinateNames.offset, new AttributeValue().withN(String.valueOf(record.kafkaOffset())));
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }

    @Override
    public String version() {
        return Version.get();
    }

}
