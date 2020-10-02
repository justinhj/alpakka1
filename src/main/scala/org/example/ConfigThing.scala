package org.example

import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.actor.ActorSystem
import akka.stream.Materializer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ConfigThing extends App {

  def serviceStreamWithConfigMap[CK, CV, E, M](
    staticConfig:  Source[(CK, CV), M],
    dynamicConfig: Source[(CK, CV), M],
    serviceStream: Source[E, M]) : Source[(E, Map[CK, CV]), M] =

      staticConfig
        .fold(Right[(CK, CV), Map[CK, CV]](Map.empty)) {
          case (Right(config), (k, v)) => Right(config.updated(k, v))
        }
        .concat(dynamicConfig.map(Left.apply))
        .scan(Right[E, Map[CK, CV]](Map.empty[CK, CV])) {
          case (_,             Right(config)) => {
            Console.println("initial config")
            Right(config)
          }
          case (Right(config), Left((k, v)))  => {
            Console.println("updated (dynamic) config")
            Right(config.updated(k, v))
          }
        }
        .merge(serviceStream.map(Left.apply))
        .scan((Option.empty[E], Map.empty[CK, CV])) {
          case (_,           Right(config)) => None    -> config
          case ((_, config), Left(e))       => Some(e) -> config
        }
        .collect {
          case (Some(e), config) => (e, config)
        }


  val actorSystem = ActorSystem("as")
  implicit val mar = Materializer(actorSystem)

  case class ServiceEvent(configName: String, baseValue: Int)

  val static: Source[(String, Int), NotUsed] = Source(List
    (("a", 10), ("b", -5), ("c", 12)))

  val dynamicConfigs: Source[(String, Int), NotUsed] = Source(List
    (("e", 20), ("f", -50)))

  val dynamic = Source.tick(3 seconds, 3 seconds, ()).zip(dynamicConfigs).map{
    case (_, config) => config
  }

  val serviceEvents: Source[ServiceEvent, NotUsed] = Source(
    List(
      ServiceEvent("a", 3),
      ServiceEvent("a", 3),
      ServiceEvent("a", 3),
      ServiceEvent("a", 3),
      ServiceEvent("a", 3),
      ServiceEvent("b", 10),
      ServiceEvent("e", 30)))

  val service = Source.tick(0 seconds, 3 seconds, ()).zip(serviceEvents).map{
    case (_, event) => event
  }

  Console.println("hello")

  val graph = serviceStreamWithConfigMap(static, dynamic, service).
        runWith(Sink.foreach({
          case (ev, config) => // (ServiceEvent, Map[String,Int]))
            val c = config.get(ev.configName)
            Console.println(s"Event key ${ev.configName} base ${ev.baseValue} config opt $c")
        }))

  val result = Await.result(graph, 30 seconds)

}