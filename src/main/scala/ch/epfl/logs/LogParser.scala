package ch.epfl.logs

import scala.util.parsing.combinator.RegexParsers

/**
 * Created by renucci on 22/03/15.
 */
object LogParser extends RegexParsers with java.io.Serializable {

  def logLine: Parser[Log] = (
        appSummary
      | containerAllocated
      | containerKilled
//      | exitedWithSuccessContainer
      | memoryUsage)

  private def appSummary: Parser[AppSummary] = (
    timestamp
      ~ "INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_" ~ appId
      ~ ",name=" ~ identW
      ~ ",user=" ~ ident
      ~ ",queue=default,state=" ~ ident
      ~ ",trackingUrl=" ~ url
      ~ ",appMasterHost=" ~ ident
      ~ ".icdatacluster2,startTime=" ~ ident
      ~ ",finishTime=" ~ ident
      ~ ",finalStatus=" ~ ident  ^^ {
      case t ~ _ ~ id ~ _ ~ name ~ _ ~ user ~ _ ~ state ~_ ~ url ~ _ ~ host ~ _ ~ stime ~ _ ~ etime ~ _ ~ finalStatus =>
        AppSummary(t, id, name, user, state, url, host, stime.toLong, etime.toLong, finalStatus)
    })

  private def containerAllocated: Parser[AllocatedContainer] = (
    timestamp
      ~ "INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_"
      ~ appId ~ ident
      ~ "Container Transitioned from NEW to ALLOCATED" ^^ {
      case t ~ _ ~ id  ~ contId ~ _ =>
        AllocatedContainer(t, id, id + contId)
    })

  private def containerKilled: Parser[AllocatedContainer] = (
    timestamp
      ~ "INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_"
      ~ appId ~ ident
      ~ "Container Transitioned from RUNNING to KILLED" ^^ {
      case t ~ _ ~ id  ~ contId ~ _ =>
        AllocatedContainer(t, id, id + contId)
    })

  private def exitedWithSuccessContainer: Parser[ExitedWithSuccessContainer] = (
    timestamp
      ~ "INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_"
      ~ appId ~ ident
      ~ "Container Transitioned from RUNNING to EXITED_WITH_SUCCESS" ^^ {
      case t ~ _ ~ id  ~ contId ~ _ =>
        ExitedWithSuccessContainer(t, id, id + contId)
    })

  private def memoryUsage: Parser[MemoryUsage] = (
    timestamp
      ~ "INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl: Memory usage of ProcessTree"
      ~ ident
      ~ "for container-id container_" ~ appId ~ ident ~ ":"
      ~ double ~ size ~ "of" ~ double ~ size ~ "physical memory used;"
      ~ double ~ size ~ "of" ~ double ~ size ~ "virtual memory used" ^^ {
      case t ~ _ ~ procId ~ _ ~ id ~ contId ~ _ ~ pMemUsed ~ pMemUsedSize ~ _ ~ pMemTot ~ pMemTotSize
        ~ _ ~ vMemUsed ~ vMemUsedSize ~ _ ~ vMemTot ~ vMemTotSize ~ _ =>
        val pMem = convert(pMemUsed.toDouble, pMemUsedSize) / convert(pMemTot.toDouble, pMemTotSize)
        val vMem = convert(vMemUsed.toDouble, vMemUsedSize) / convert(vMemTot.toDouble, vMemTotSize)
        MemoryUsage(t, id, id + contId, pMem, vMem)
    })



  val size: Parser[String] = "MB" | "GB"
  val double: Parser[String] = "\\d+.\\d+".r
  val appId: Parser[String] = "\\d+_\\d+".r
  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r

  private def convert(d: Double, unity: String) = unity match {
    case "GB" => d * 1000
    case "MB" => d
    case _    => 0.0
  }
}
