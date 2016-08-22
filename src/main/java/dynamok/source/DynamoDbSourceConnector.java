package dynamok.source;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
            return taskConfig;
        }).collect(Collectors.toList());
    }

    @Override
    public void start(Map<String, String> props) {
        config = new ConnectorConfig(props);
        streamShards = new HashMap<>();

        final AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        client.configureRegion(config.region);

        final AmazonDynamoDBStreamsClient streamsClient = new AmazonDynamoDBStreamsClient();
        streamsClient.configureRegion(config.region);

        final Set<String> ignoredTables = new HashSet<>();
        final Set<String> consumeTables = new HashSet<>();

        String lastEvaluatedTableName = null;
        do {
            final ListTablesResult listResult = client.listTables(lastEvaluatedTableName);

            for (String tableName: listResult.getTableNames()) {
                if ((config.tablesPrefix == null || tableName.startsWith(config.tablesPrefix)) &&
                        (config.tablesBlacklist == null || !config.tablesBlacklist.contains(tableName)) &&
                        (config.tablesWhitelist == null || config.tablesWhitelist.contains(tableName))) {
                    ignoredTables.add(tableName);
                    continue;
                }

                final TableDescription tableDesc = client.describeTable(tableName).getTable();

                if (!tableDesc.getStreamSpecification().isStreamEnabled()) {
                    throw new ConnectException(String.format("DynamoDB table `%s` does not have streams enabled", tableName));
                }

                final String streamViewType = tableDesc.getStreamSpecification().getStreamViewType();
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

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return ConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

}
