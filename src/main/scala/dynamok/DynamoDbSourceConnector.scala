package dynamok

import java.util.{List => JList, Map => JMap}

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{DescribeStreamRequest, DescribeStreamResult, Shard, StreamDescription, StreamViewType, TableDescription}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBClient, AmazonDynamoDBStreamsClient}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class DynamoDbSourceConnector extends SourceConnector {

  import DynamoDbSourceConnector._

  val log = LoggerFactory.getLogger(classOf[DynamoDbSourceConnector])

  var region: Regions = _
  var tableDescription: TableDescription = _
  var shards: JList[Shard] = _
  var destTopic: String = _

  override def taskClass(): Class[_ <: Task] = classOf[DynamoDbSourceTask]

  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = {
    ConnectorUtils.groupPartitions(shards, maxTasks).asScala.map {
      taskShards =>
        Map(
          ConfigKeys.Region -> region.getName,
          ConfigKeys.Table -> tableDescription.getTableName,
          ConfigKeys.Shards -> taskShards.asScala.map(_.getShardId).mkString(","),
          ConfigKeys.StreamArn -> tableDescription.getLatestStreamArn,
          ConfigKeys.Topic -> destTopic
        ).asJava
    }.toList.asJava
  }

  override def start(props: JMap[String, String]): Unit = {
    val config = ConnectorConfig(props)

    val client = new AmazonDynamoDBClient()
    client.configureRegion(config.region)

    tableDescription = client.describeTable(config.table).getTable
    if (!tableDescription.getStreamSpecification.isStreamEnabled) {
      sys.error("The DynamoDB table does not have streams enabled")
    }

    val streamViewType = tableDescription.getStreamSpecification.getStreamViewType
    if (!AcceptableStreamViewTypes.contains(streamViewType)) {
      sys.error(s"""The DynamoDB table's stream view type needs to be one of ${AcceptableStreamViewTypes.mkString("{,", ",", "}")}, was: ${streamViewType}""")
    }

    region = config.region

    val streamsClient: AmazonDynamoDBStreamsClient = new AmazonDynamoDBStreamsClient()
    streamsClient.configureRegion(region)

    val describeStreamResult: DescribeStreamResult =
      streamsClient.describeStream(new DescribeStreamRequest().withStreamArn(tableDescription.getLatestStreamArn))

    val streamDescription: StreamDescription = describeStreamResult.getStreamDescription
    shards = streamDescription.getShards

    destTopic = config.topic
  }

  override def stop(): Unit = {
  }

  override def config(): ConfigDef = ConfigDefinition

  override def version(): String = AppInfoParser.getVersion

}

object DynamoDbSourceConnector {

  val AcceptableStreamViewTypes = Seq(
    StreamViewType.NEW_IMAGE.name(),
    StreamViewType.NEW_AND_OLD_IMAGES.name()
  )

  object ConfigKeys {
    val Region = "region"
    val Table = "table"
    val Shards = "shards"
    val StreamArn = "stream_arn"
    val Topic = "topic"
  }

  val ConfigDefinition = new ConfigDef()
    .define(ConfigKeys.Region, Type.STRING, Importance.HIGH, "AWS region.")
    .define(ConfigKeys.Table, Type.STRING, Importance.HIGH, "Source DynamoDB table.")
    .define(ConfigKeys.Topic, Type.STRING, Importance.HIGH, "Destination Kafka topic.")

  case class ConnectorConfig(props: JMap[String, String]) {

    lazy val region: Regions = Regions.fromName(props.get(ConfigKeys.Region))

    lazy val table: String = props.get(ConfigKeys.Table)

    lazy val shards: Seq[String] = props.get(ConfigKeys.Shards).split(',').filterNot(_.isEmpty)

    lazy val streamArn: String = props.get(ConfigKeys.StreamArn)

    lazy val topic: String = props.get(ConfigKeys.Topic)

  }

}
