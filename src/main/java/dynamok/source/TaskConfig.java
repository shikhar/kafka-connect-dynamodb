package dynamok.source;

import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TaskConfig extends AbstractConfig {

    enum Keys {
        ;

        static String REGION = "region";
        static String TOPIC_FORMAT = "topic.format";
        static String SHARDS = "shards";
        static String TABLE = "table";
        static String STREAM_ARN = "stream.arn";
    }

    final Regions region;
    final String topicFormat;
    final List<String> shards;

    TaskConfig(Map<String, String> props) {
        super((Map) props);
        region = Regions.fromName(getString(Keys.REGION));
        topicFormat = getString(Keys.TOPIC_FORMAT);
        shards = Arrays.stream(getString(Keys.SHARDS).split(",")).filter(shardId -> !shardId.isEmpty()).collect(Collectors.toList());
    }

    String tableForShard(String shardId) {
        return getString(shardId + "." + Keys.TABLE);
    }

    String streamArnForShard(String shardId) {
        return getString(shardId + "." + Keys.STREAM_ARN);
    }

}
