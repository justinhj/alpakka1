package org.example

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import org.apache.kafka.common.serialization.StringDeserializer
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
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Source
import akka.stream.Graph
import akka.stream.SinkShape
import akka.stream.scaladsl.Flow

object Alpakka1 extends App {

  def divertOnFailure[F,S,M](source: Source[Either[F,S], M],
    successSink : Graph[SinkShape[S], _],
    failureSink : Graph[SinkShape[F], _]): RunnableGraph[M] = {
    source.
      divertTo(Flow[Either[F,S]].map[F]{
          case Left(result) => result
        }.to(failureSink), _.isLeft).
      map{
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
      .withGroupId("group2")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  consumerSettings
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val committerDefaults = CommitterSettings(system)

//  val failureSink = Flow[String].
//    map{
//      oops =>
//        system.log.error(s"We got an oops $oops")
//    }.toMat(Committer.sink(committerDefaults))(DrainingControl.apply)

  val control =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1){
        //
        msg : CommittableMessage[String,String] =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
      }
      .toMat(Committer.sink(committerDefaults))(DrainingControl.apply)
      .run()

  def business(key: String, value: String): Future[Done] = {
    Future(system.log.info(s"key $key value $value")).map(_ => Done)
  }

}
