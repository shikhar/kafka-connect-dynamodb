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

import scala.collection.JavaConverters._

class DynamoDbSourceConnector extends SourceConnector {

  import DynamoDbSourceConnector._

  var regions: Regions = _
  var tableDescription: TableDescription = _
  var shards: JList[Shard] = _

  override def taskClass(): Class[_ <: Task] = classOf[DynamoDbSourceTask]

  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = {
    val numShardsPerTask = math.max(shards.size / maxTasks, 1)

    shards.asScala.grouped(numShardsPerTask).map {
      taskShards =>
        Map(
          ConfigKeys.Region -> regions.getName,
          ConfigKeys.Table -> tableDescription.getTableName,
          ConfigKeys.Shards -> taskShards.map(_.getShardId).mkString(","),
          ConfigKeys.StreamArn -> tableDescription.getLatestStreamArn
        ).asJava
    }.asJava
  }

  override def start(props: JMap[String, String]): Unit = {
    val region = Regions.fromName(props.get(ConfigKeys.Region))
    val table = props.get(ConfigKeys.Table)

    val client = new AmazonDynamoDBClient()
    client.configureRegion(region)

    tableDescription = client.describeTable(table).getTable

    if (!tableDescription.getStreamSpecification.isStreamEnabled) {
      // TODO throw
    }
    if (StreamViewType.fromValue(tableDescription.getStreamSpecification.getStreamViewType) != StreamViewType.NEW_AND_OLD_IMAGES) {
      // TODO throw
    }

    val streamsClient = new AmazonDynamoDBStreamsClient()
    streamsClient.configureRegion(region)

    val describeStreamResult: DescribeStreamResult = streamsClient.describeStream(new DescribeStreamRequest().withStreamArn(tableDescription.getLatestStreamArn))

    val streamDescription: StreamDescription = describeStreamResult.getStreamDescription
    shards = streamDescription.getShards
  }

  override def stop(): Unit = {
  }

  override def config(): ConfigDef = ConfigDef

  override def version(): String = AppInfoParser.getVersion

}

object DynamoDbSourceConnector {

  object ConfigKeys {
    val Region = "region"
    val Table = "table"
    val Shards = "shards"
    val StreamArn = "stream_arn"
  }

  val ConfigDef = new ConfigDef()
    .define(ConfigKeys.Region, Type.STRING, Importance.HIGH, "AWS region.")
    .define(ConfigKeys.Table, Type.STRING, Importance.HIGH, "Source DynamoDB table.")

}
