package dynamok.source;

import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

class ConnectorConfig extends AbstractConfig {

    private enum Keys {
        ;
        static final String REGION = "region";
        static final String TABLES_PREFIX = "tables.regex";
        static final String TABLES_WHITELIST = "tables.whitelist";
        static final String TABLES_BLACKLIST = "tables.blacklist";
        static final String TOPIC_FORMAT = "topic.format";
    }

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(Keys.REGION, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (key, regionName) -> {
                if (!Arrays.stream(Regions.values()).anyMatch(x -> x.getName().equals(regionName))) {
                    throw new ConfigException("Invalid AWS region: " + regionName);
                }
            }, ConfigDef.Importance.HIGH, "AWS region for the source DynamoDB.")
            .define(Keys.TABLES_PREFIX, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.MEDIUM, "Prefix for DynamoDB tables to source from.")
            .define(Keys.TABLES_WHITELIST, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.MEDIUM, "Whitelist for DynamoDB tables to source from.")
            .define(Keys.TABLES_BLACKLIST, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.MEDIUM, "Blacklist for DynamoDB tables to source from.")
            .define(Keys.TOPIC_FORMAT, ConfigDef.Type.STRING, "${table}",
                    ConfigDef.Importance.HIGH, "Format string for destination Kafka topic, use ``${table}`` as placeholder for source table name.");

    final Regions region;
    final String topicFormat;
    final String tablesPrefix;
    final List<String> tablesWhitelist;
    final List<String> tablesBlacklist;

    ConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        region = Regions.fromName(getString(Keys.REGION));
        tablesPrefix = getString(Keys.TABLES_PREFIX);
        tablesWhitelist = getList(Keys.TABLES_WHITELIST);
        tablesBlacklist = getList(Keys.TABLES_BLACKLIST);
        topicFormat = getString(Keys.TOPIC_FORMAT);
    }

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toRst());
    }

}
