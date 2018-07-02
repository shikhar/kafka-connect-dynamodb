/*
 * Copyright 2016 Shikhar Bhushan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dynamok.source;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.*;
import dynamok.Version;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamoDbSourceTask extends SourceTask {

    private enum Keys {
        ;

        static final String SHARD = "shard";
        static final String SEQNUM = "seqNum";
    }
    private static final String REMOVE = "REMOVE";
    private static final String OPERATIONTYPE = "OperationType";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private TaskConfig config;
    private AmazonDynamoDBStreamsClient streamsClient;

    private List<String> assignedShards;
    private Map<String, String> shardIterators;
    private int currentShardIdx;

    @Override
    public void start(Map<String, String> props) {
        config = new TaskConfig(props);

        if (config.accessKeyId.isEmpty() || config.secretKey.isEmpty()) {
            streamsClient = new AmazonDynamoDBStreamsClient(DefaultAWSCredentialsProviderChain.getInstance());
            log.debug("AmazonDynamoDBStreamsClient created with DefaultAWSCredentialsProviderChain");
        } else {
            final BasicAWSCredentials awsCreds = new BasicAWSCredentials(config.accessKeyId, config.secretKey);
            streamsClient = new AmazonDynamoDBStreamsClient(awsCreds);
            log.debug("AmazonDynamoDBStreamsClient created with AWS credentials from connector configuration");
        }

        streamsClient.configureRegion(config.region);

        assignedShards = new ArrayList<>(config.shards);
        shardIterators = new HashMap<>(assignedShards.size());
        currentShardIdx = 0;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // TODO rate limiting?

        if (assignedShards.isEmpty()) {
            throw new ConnectException("No remaining source shards");
        }

        final String shardId = assignedShards.get(currentShardIdx);

        final GetRecordsRequest req = new GetRecordsRequest();
        req.setShardIterator(shardIterator(shardId));
        req.setLimit(100); // TODO configurable

        final GetRecordsResult rsp = streamsClient.getRecords(req);
        if (rsp.getNextShardIterator() == null) {
            log.info("Shard ID `{}` for table `{}` has been closed, it will no longer be polled", shardId, config.tableForShard(shardId));
            shardIterators.remove(shardId);
            assignedShards.remove(shardId);
        } else {
            log.debug("Retrieved {} records from shard ID `{}`", rsp.getRecords().size(), shardId);
            shardIterators.put(shardId, rsp.getNextShardIterator());
        }

        currentShardIdx = (currentShardIdx + 1) % assignedShards.size();

        final String tableName = config.tableForShard(shardId);
        final String topic = config.topicFormat.replace("${table}", tableName);
        final Map<String, String> sourcePartition = sourcePartition(shardId);

        try {
            return rsp.getRecords().stream()
                    .map(dynamoRecord -> toSourceRecord(sourcePartition, topic, dynamoRecord))
                    .collect(Collectors.toList());
        }

        catch (Exception exception) {
            log.error("Failed to stream data into Kafka {}", exception.toString());
            return null;
        }
    }

    private SourceRecord toSourceRecord(Map<String, String> sourcePartition, String topic, Record record) {
        StreamRecord dynamoRecord = record.getDynamodb();
        log.info(config.isRecordStreamLogOn.toString());
       if(config.isRecordStreamLogOn) {
           log.info(record.toString());
       }

        Map<String, AttributeValue> keyAttributeMap = dynamoRecord.getKeys();
        keyAttributeMap.put(OPERATIONTYPE,new AttributeValue().withS(record.getEventName()) );
        Map<String, AttributeValue> valueAttributeMap =  record.getEventName().equals(REMOVE)? new HashMap<>() :dynamoRecord.getNewImage();
            return new SourceRecord(
                    sourcePartition,
                    Collections.singletonMap(Keys.SEQNUM, dynamoRecord.getSequenceNumber()),
                    topic, null,
                    RecordMapper.attributesSchema(), RecordMapper.toConnect(keyAttributeMap),
                    RecordMapper.attributesSchema(), RecordMapper.toConnect(valueAttributeMap),
                    dynamoRecord.getApproximateCreationDateTime().getTime()
            );

    }

    private String shardIterator(String shardId) {
        String iterator = shardIterators.get(shardId);
        if (iterator == null) {
            final GetShardIteratorRequest req = getShardIteratorRequest(
                    shardId,
                    config.streamArnForShard(shardId),
                    storedSequenceNumber(sourcePartition(shardId))
            );
            iterator = streamsClient.getShardIterator(req).getShardIterator();
            shardIterators.put(shardId, iterator);
        }
        return iterator;
    }

    private Map<String, String> sourcePartition(String shardId) {
        return Collections.singletonMap(Keys.SHARD, shardId);
    }

    private String storedSequenceNumber(Map<String, String> partition) {
        final Map<String, Object> offsetMap = context.offsetStorageReader().offset(partition);
        return offsetMap != null ? (String) offsetMap.get(Keys.SEQNUM) : null;
    }

    private GetShardIteratorRequest getShardIteratorRequest(
            String shardId,
            String streamArn,
            String seqNum
    ) {
        final GetShardIteratorRequest req = new GetShardIteratorRequest();
        req.setShardId(shardId);
        req.setStreamArn(streamArn);
        if (seqNum == null) {
            req.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else {
            req.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            req.setSequenceNumber(seqNum);
        }
        return req;
    }

    @Override
    public void stop() {
        if (streamsClient != null) {
            streamsClient.shutdown();
            streamsClient = null;
        }
    }

    @Override
    public String version() {
        return Version.get();
    }

}
