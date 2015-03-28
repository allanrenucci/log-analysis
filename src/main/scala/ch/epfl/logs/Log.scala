package ch.epfl.logs

sealed trait Log extends Serializable

abstract class ApplicationLog extends Log {
  val appId: String
}

case class AppSummary(timestamp: String, appId: String, name: String, user: String,
                      state:String, url:String, host: String, startTime: Long,
                      endTime: Long, finalStatus: String) extends ApplicationLog

case class AllocatedContainer(timestamp: String, appId: String,
                              contId: String) extends ApplicationLog

case class KilledContainer(timestamp: String, appId: String,
                              contId: String) extends ApplicationLog

case class ExitedWithSuccessContainer(timestamp: String, appId: String,
                           contId: String) extends ApplicationLog

case class MemoryUsage(timestamp: String, appId: String, contId: String,
                       physical: Double, virtual: Double) extends ApplicationLog

case object Undefined extends Log
