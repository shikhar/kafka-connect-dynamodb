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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import dynamok.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DynamoDbSourceConnector extends SourceConnector {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private ConnectorConfig config;
    private Map<Shard, TableDescription> streamShards;

    @Override
    public Class<? extends Task> taskClass() {
        return DynamoDbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return ConnectorUtils.groupPartitions(new ArrayList<>(streamShards.keySet()), maxTasks).stream().map(taskShards -> {
            final Map<String, String> taskConfig = new HashMap<>();
            taskConfig.put(TaskConfig.Keys.REGION, config.region.getName());
            taskConfig.put(TaskConfig.Keys.TOPIC_FORMAT, config.topicFormat);
            taskConfig.put(TaskConfig.Keys.SHARDS, taskShards.stream().map(Shard::getShardId).collect(Collectors.joining(",")));
            taskShards.forEach(shard -> {
                final TableDescription tableDesc = streamShards.get(shard);
                taskConfig.put(shard.getShardId() + "." + TaskConfig.Keys.TABLE, tableDesc.getTableName());
                taskConfig.put(shard.getShardId() + "." + TaskConfig.Keys.STREAM_ARN, tableDesc.getLatestStreamArn());
            });
            taskConfig.put(TaskConfig.Keys.LOG_RECORD_STREAM, config.isRecordStreamLogOn);
            return taskConfig;
        }).collect(Collectors.toList());
    }

    @Override
    public void start(Map<String, String> props) {
        config = new ConnectorConfig(props);
        streamShards = new HashMap<>();

        final AmazonDynamoDBClient client;
        final AmazonDynamoDBStreamsClient streamsClient;

        if (config.accessKeyId.value().isEmpty() || config.secretKey.value().isEmpty()) {
            client = new AmazonDynamoDBClient(DefaultAWSCredentialsProviderChain.getInstance());
            streamsClient = new AmazonDynamoDBStreamsClient(DefaultAWSCredentialsProviderChain.getInstance());
            log.debug("AmazonDynamoDBStreamsClient created with DefaultAWSCredentialsProviderChain");
        } else {
            final BasicAWSCredentials awsCreds = new BasicAWSCredentials(config.accessKeyId.value(), config.secretKey.value());
            client = new AmazonDynamoDBClient(awsCreds);
            streamsClient = new AmazonDynamoDBStreamsClient(awsCreds);
            log.debug("AmazonDynamoDB clients created with AWS credentials from connector configuration");
        }

        client.configureRegion(config.region);
        streamsClient.configureRegion(config.region);

        final Set<String> ignoredTables = new HashSet<>();
        final Set<String> consumeTables = new HashSet<>();

        String lastEvaluatedTableName = null;
        do {
            final ListTablesResult listResult = client.listTables(lastEvaluatedTableName);

            for (String tableName : listResult.getTableNames()) {
                if (!acceptTable(tableName)) {
                    ignoredTables.add(tableName);
                    continue;
                }

                final TableDescription tableDesc = client.describeTable(tableName).getTable();

                final StreamSpecification streamSpec = tableDesc.getStreamSpecification();

                if (streamSpec == null || !streamSpec.isStreamEnabled()) {
                    throw new ConnectException(String.format("DynamoDB table `%s` does not have streams enabled", tableName));
                }

                final String streamViewType = streamSpec.getStreamViewType();
                if (!streamViewType.equals(StreamViewType.NEW_IMAGE.name()) && !streamViewType.equals(StreamViewType.NEW_AND_OLD_IMAGES.name())) {
                    throw new ConnectException(String.format("DynamoDB stream view type for table `%s` is %s", tableName, streamViewType));
                }

                final DescribeStreamResult describeStreamResult =
                        streamsClient.describeStream(new DescribeStreamRequest().withStreamArn(tableDesc.getLatestStreamArn()));

                for (Shard shard : describeStreamResult.getStreamDescription().getShards()) {
                    streamShards.put(shard, tableDesc);
                }

                consumeTables.add(tableName);
            }

            lastEvaluatedTableName = listResult.getLastEvaluatedTableName();
        } while (lastEvaluatedTableName != null);

        log.info("Tables to ignore: {}", ignoredTables);
        log.info("Tables to ingest: {}", consumeTables);

        client.shutdown();
        streamsClient.shutdown();
    }

    private boolean acceptTable(String tableName) {
        return tableName.startsWith(config.tablesPrefix)
                && (config.tablesWhitelist.isEmpty() || config.tablesWhitelist.contains(tableName))
                && !config.tablesBlacklist.contains(tableName);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return ConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.get();
    }

}
