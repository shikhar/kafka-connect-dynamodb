package dynamok

import java.util.{List => JList, Map => JMap}

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class DynamoDbSourceConnector extends SourceConnector {

  override def taskClass(): Class[_ <: Task] = classOf[DyanamoDbSourceTask]

  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = ???

  override def stop(): Unit = ???

  override def config(): ConfigDef = ???

  override def start(props: JMap[String, String]): Unit = ???

  override def version(): String = ???

}
