package org.example

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.kafka.scaladsl.Consumer
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Committer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.scaladsl.Sink
import akka.kafka.CommitterSettings
import akka.stream.Materializer
import scala.concurrent.Future
import akka.Done
import com.typesafe.config.ConfigFactory
import akka.kafka.ProducerSettings
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import akka.kafka.scaladsl.Producer
import akka.kafka.ProducerMessage
import akka.kafka.ProducerMessage.MultiResultPart

object Alpakka2 extends App {

  // A sample consumer
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  val config = ConfigFactory.load()

  val system = ActorSystem("producer", config)
  implicit val materializer = Materializer(system)

  val producerConfig = system.settings.config.getConfig("akka.kafka.producer")

  val producerSettings =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

  // Simple producer as a sink

  // val done: Future[Done] =
  //   Source(1 to 100)
  //     .map(n => s"Message $n yo!")
  //     .map(value => new ProducerRecord[String, String]("topic1", value))
  //     .runWith(Producer.plainSink(producerSettings))

  // FlexiFlow

 val done = Source(1 to 10000)
  .map { number =>
    val partition = 0
    val value = number.toString
    ProducerMessage.single(
      new ProducerRecord("topic1", partition, s"key-$value", value),
      number * number
    )
  }
  .via(Producer.flexiFlow(producerSettings))
  .map {
    case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) =>
      s"${metadata.topic}/${metadata.partition} ${metadata.offset}: ${record.value} passThrough $passThrough"

    case ProducerMessage.MultiResult(parts, passThrough) =>
      parts
        .map {
          case MultiResultPart(metadata, record) =>
            s"${metadata.topic}/${metadata.partition} ${metadata.offset}: ${record.value} passThrough $passThrough"
        }
        .mkString(", ")

    case ProducerMessage.PassThroughResult(passThrough) =>
      s"passed through $passThrough"
  }
  .runWith(Sink.foreach(println(_)))

}
