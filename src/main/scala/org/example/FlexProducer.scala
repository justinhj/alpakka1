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

object FlexProducer extends App {

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

 val stream = Source(1 to 10)
  .map { number =>
    val partition = 0
    val value = number.toString
    ProducerMessage.single(
      new ProducerRecord("topic1", partition, s"key-$value", value),
      number * number
    )
  }
   // Flexiflow producer let's us write the message to a stream and then continue with the message
   // doing processing. You can use this to pass through committer offsets from a Source, for example,
   // and then commit only after you've produced a new value in some other broker/topic
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

  val f = stream.runWith(Sink.foreach(println(_)))

  f.onComplete(_ -> system.terminate())

}
