package dynamok

import java.util.{Collections, List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient
import com.amazonaws.services.dynamodbv2.model.{GetRecordsRequest, GetRecordsResult, GetShardIteratorRequest, ShardIteratorType, StreamRecord}
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class DynamoDbSourceTask extends SourceTask {

  import DynamoDbSourceConnector.ConnectorConfig

  val log = LoggerFactory.getLogger(classOf[DynamoDbSourceTask])

  val streamsClient: AmazonDynamoDBStreamsClient = new AmazonDynamoDBStreamsClient()

  val shards: mutable.ArrayBuffer[String] = mutable.ArrayBuffer()

  var streamArn: String = _

  var topic: String = _

  val sourcePartitions: mutable.Map[String, JMap[String, String]] = mutable.Map()

  val shardIterators: mutable.Map[String, String] = mutable.Map()

  var currentShardIdx: Int = 0

  override def start(props: JMap[String, String]): Unit = {
    val config = ConnectorConfig(props)
    streamsClient.configureRegion(config.region)
    shards ++= config.shards
    streamArn = config.streamArn
    topic = config.topic
    log.info(s"Initialized for shard IDs ${shards.mkString("[", ", ", "]")}")
  }

  override def poll(): JList[SourceRecord] = {
    // TODO rate limiting?

    if (shards.isEmpty) {
      sys.error("No remaining source shards")
    }

    val shardId = shards(currentShardIdx)

    val req = new GetRecordsRequest()
    req.setShardIterator(shardIterator(shardId))
    req.setLimit(100) // TODO configurable

    val rsp: GetRecordsResult = streamsClient.getRecords(req)

    Option(rsp.getNextShardIterator) match {
      case Some(nextShardIterator) =>
        shardIterators(shardId) = nextShardIterator
      case None =>
        log.info(s"Shard ID ${shardId} has been closed, it will no longer be polled")
        shardIterators -= shardId
        shards -= shardId
    }

    currentShardIdx = (currentShardIdx + 1) % shards.length

    if (rsp.getRecords.isEmpty) {
      null
    } else {
      val partition: JMap[String, String] = sourcePartition(shardId)
      rsp.getRecords.asScala.map {
        record =>
          toSourceRecord(partition, record.getDynamodb)
      }.asJava
    }
  }

  private def toSourceRecord(partition: JMap[String, String], dynamoRecord: StreamRecord): SourceRecord = {
    // TODO schemanating
    new SourceRecord(
      partition, toSourceOffsetMap(dynamoRecord.getSequenceNumber),
      topic,
      Schema.STRING_SCHEMA, dynamoRecord.getKeys.toString,
      Schema.STRING_SCHEMA, dynamoRecord.getNewImage.toString
    )
  }

  private def shardIterator(shardId: String): String = {
    shardIterators.getOrElseUpdate(shardId, {
      val req = getShardIteratorRequest(shardId, streamArn, storedSequenceNumber(sourcePartition(shardId)))
      streamsClient.getShardIterator(req).getShardIterator
    })
  }

  private def sourcePartition(shardId: String): JMap[String, String] = {
    sourcePartitions.getOrElseUpdate(shardId, {
      Collections.singletonMap("shard", shardId)
    })
  }

  private def toSourceOffsetMap(seqNum: String): JMap[String, String] = {
    Collections.singletonMap("seqnum", seqNum)
  }

  private def fromSourceOffsetMap(offset: JMap[String, _]): Option[String] = {
    Option(offset.get("seqnum")).map(_.toString)
  }

  private def getShardIteratorRequest(shardId: String, streamArn: String, seqNum: Option[String]): GetShardIteratorRequest = {
    val req = new GetShardIteratorRequest()
    req.setShardId(shardId)
    req.setStreamArn(streamArn)
    seqNum match {
      case None =>
        req.setShardIteratorType(ShardIteratorType.TRIM_HORIZON)
      case Some(sn) =>
        req.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
        req.setSequenceNumber(sn)
    }
    req
  }

  private def storedSequenceNumber(partition: JMap[String, _]): Option[String] = {
    Option(context.offsetStorageReader().offset(partition))
      .flatMap {
        fromSourceOffsetMap
      }
  }

  override def stop(): Unit = {
    streamsClient.shutdown()
  }

  override def version(): String = AppInfoParser.getVersion

}
