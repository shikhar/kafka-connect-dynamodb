package dynamok

import java.util

import org.apache.kafka.connect.connector.Task

class DyanamoDbSourceTask extends Task {

  override def stop(): Unit = ???

  override def start(props: util.Map[String, String]): Unit = ???

  override def version(): String = ???

}
