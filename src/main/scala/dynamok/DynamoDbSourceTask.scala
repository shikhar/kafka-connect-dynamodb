package dynamok

import java.util.concurrent.{BlockingQueue, ExecutorService, LinkedBlockingQueue}
import java.util.{LinkedList => JLinkedList, List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient
import com.amazonaws.services.dynamodbv2.model.{GetRecordsRequest, GetShardIteratorRequest, ShardIteratorType, Record => DynamoDbRecord}
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration

class DynamoDbSourceTask extends SourceTask {

  import DynamoDbSourceConnector.ConnectorConfig

  val log = LoggerFactory.getLogger(classOf[DynamoDbSourceTask])

  val queue: BlockingQueue[SourceRecord] = new LinkedBlockingQueue[SourceRecord](1024) // TODO configurable capacity

  @volatile var streamsClient: AmazonDynamoDBStreamsClient = _
  @volatile var executor: ExecutorService = _
  @volatile var isShutdown = false

  override def start(props: JMap[String, String]): Unit = {
    val config = ConnectorConfig(props)

    streamsClient = new AmazonDynamoDBStreamsClient()
    streamsClient.configureRegion(config.region)

    val streamArn = config.streamArn

    for (shardId <- config.shards) {
      executor.submit(new Runnable {
        override def run(): Unit = {
          // TODO error handling, should flag to poll() as well so can fail the task

          val partition = Map("shard" -> shardId).asJava

          var shardIterator = streamsClient.getShardIterator(
            getShardIteratorRequest(shardId, streamArn, storedSequenceNumber(partition))
          ).getShardIterator

          while (!isShutdown && shardIterator != null) {
            // TODO while not shutdown
            val records = streamsClient.getRecords(getRecordsRequest(shardIterator))
            enqueueSourceRecords(partition, records.getRecords)
            shardIterator = records.getNextShardIterator
          }
        }
      })
    }
  }

  override def poll(): JList[SourceRecord] = {
    val records = new JLinkedList[SourceRecord]
    records.add(records.poll()) // block for at least one
    queue.drainTo(records) // obtain as many more as possible without blocking
    records
  }

  private def enqueueSourceRecords(partition: JMap[String, _], records: JList[DynamoDbRecord]): Unit = {
    // TODO to SourceRecord; queue.add()
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
    Option(
      context.offsetStorageReader().offset(partition)
    ).flatMap {
      offset: JMap[String, AnyRef] =>
        Option(offset.get("seqnum")).map(_.toString)
    }
  }

  private def getRecordsRequest(shardIter: String): GetRecordsRequest = {
    val req = new GetRecordsRequest()
    req.setShardIterator(shardIter)
    req.setLimit(128) // TODO configurable
    req
  }

  override def stop(): Unit = {
    isShutdown = true
    if (streamsClient != null) {
      streamsClient.shutdown()
    }
    if (executor != null) {
      executor.shutdown()
      if (!executor.awaitTermination(1, duration.MINUTES)) {
        sys.error(s"Timeout awaiting shutdown of executor for ${getClass}")
      }
    }
  }

  override def version(): String = AppInfoParser.getVersion

}
