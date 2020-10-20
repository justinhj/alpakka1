package org.example

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.{Graph, Materializer, SinkShape}
import akka.stream.scaladsl.{Flow, RunnableGraph, Source}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.Future

object CommitConsumerSplit extends App {

  def divertOnFailure[F, S, M](source: Source[Either[F, S], M],
                               successSink: Graph[SinkShape[S], _],
                               failureSink: Graph[SinkShape[F], _]): RunnableGraph[M] = {
    source.
      divertTo(Flow[Either[F, S]].map[F] {
        case Left(result) => result
      }.to(failureSink), _.isLeft).
      map {
        case Right(result) => result
      }.
      to(successSink)
  }

  // A sample consumer
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  val config = ConfigFactory.load()

  val system = ActorSystem("consumer", config)
  implicit val materializer = Materializer(system)

  val consumerConfig = system.settings.config.getConfig("akka.kafka.consumer")

  val consumerSettings =
    ConsumerSettings(consumerConfig, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("splitconsumer2")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  consumerSettings
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val committerDefaults = CommitterSettings(system)

  val failureSink = Flow.fromFunction[(Either[String, String], CommittableOffset), CommittableOffset] {
      case (Left(err), co) =>
        system.log.error(s"We got an oops $err")
        co
    }
    .to(Committer.sink(committerDefaults))

  val stream =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map(a => {
          system.log.error("butt")
          a
        })
      .map {
        (msg: CommittableMessage[String, String]) =>
          val r = business(msg.record.key, msg.record.value)
          (r, msg.committableOffset)
      }
      .map(a => {
        system.log.error("ass")
        a
      })
      //.divertTo(failureSink, _._1.isRight)
      .map(_._2)
      .to(Committer.sink(committerDefaults))

    stream.run

  //      .mapAsync(1){
  //        //
  //        msg : CommittableMessage[String,String] =>
  //          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
  //      }
  //      .toMat(Committer.sink(committerDefaults))(DrainingControl.apply)
  //      .run()

  def business(key: String, value: String): Either[String,String] = {
    if (value == "6") {
      system.log.error(s"Error key $key value $value")
      Left("error value")
    } else {
      system.log.info(s"Ok    key $key value $value")
      Right(value)
    }
  }
}
