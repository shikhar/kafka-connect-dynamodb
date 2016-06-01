package dynamok

import java.util.{List => JList, Map => JMap}

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.Shard
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._

case class DynamoDbSourceTask(shards: Seq[Shard]) extends SourceTask {

  import DynamoDbSourceConnector.ConfigKeys

  override def start(props: JMap[String, String]): Unit = {
    val region = Regions.fromName(props.get(ConfigKeys.Region))
    val table = props.get(ConfigKeys.Table)
    val shards = props.get(ConfigKeys.Shards).split(',').filterNot(_.isEmpty)
    val streamArn = props.get(ConfigKeys.StreamArn)
  }

  override def poll(): JList[SourceRecord] = {
    List().asJava
  }

  override def stop(): Unit = {

  }

  override def version(): String = AppInfoParser.getVersion

}
