/*
 * Copyright (2017) IBM Cooporation
 * Author: Michael Kaufmann (kau@zurich.ibm.com)
 */

package org.apache.spark.scheduler

import java.io.{DataOutputStream, FileNotFoundException}
import java.net.{ConnectException, HttpURLConnection, SocketException, URL}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import javax.servlet.http.HttpServletResponse

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.control.NonFatal
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException, TaskNotSerializableException, scheduler,
SPARK_VERSION => sparkVersion}
import org.apache.spark.deploy.rest
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.LaunchTask
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{SerializableBuffer, Utils}
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, Utils}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
// import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

//import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global

// Those classes need to be define outside of any other class or you'll get an
// "org.json4s.package$MappingException: unknown error"

case class HCSExecutor(id: String, host: String, rc: String)

case class HCSSchedule(taskIndex: String, execId: String, host: String, startTime: String)

case class HCSSchedules(schedule: Map[String, HCSSchedule], rc: String, seqno: String)

private[spark]
class HcsSchedulerClient(val sc: SparkContext, val schedulerBackend: SchedulerBackend)
  extends SparkListener with Logging {
  implicit val formats = DefaultFormats

  logInfo("[HCS] Instantiating HCS Scheduler Client.")
  sc.addSparkListener(this)

  private val baseURL = "http://zac25:4242/v0"

  private val registeredExecutors = new mutable.HashSet[Tuple2[String, String]]

  private val jobDependencies = new mutable.HashMap[Int, List[ActiveJob]]

  private val taskLauncher: CoarseGrainedSchedulerBackend = schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
  private val taskScheduler: TaskSchedulerImpl = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]

  private val executorTaskQueue = new HashMap[String, mutable.Queue[Tuple4[Task[_], TaskInfo, TaskDescription,
    TaskSetManager]]]

  private val executorLoad = new HashMap[String, AtomicInteger]
  private val executorCores = new HashMap[String, Int]
  private val executorIdToHost = new HashMap[String, String]

  val clock: Clock = new SystemClock()

  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  // SPARK-21563 make a copy of the jars/files so they are consistent across the TaskSet
  private val addedJars = HashMap[String, Long](sc.addedJars.toSeq: _*)
  private val addedFiles = HashMap[String, Long](sc.addedFiles.toSeq: _*)

  def executeStage(taskSetManager: TaskSetManager,
                   schedule: Map[String, Array[Long]]): HashMap[Long, TaskInfo] = {
    val taskSet = taskSetManager.taskSet
    var startedTaskIds = new mutable.HashMap[Long, TaskInfo]()
    for (index <- 0 until taskSet.tasks.size) {
      val taskSchedule = schedule.get(index.toString)
      if (taskSchedule.isDefined) {
        val task = taskSet.tasks(index)
        val taskId = taskScheduler.newTaskId()
        val taskName = s"task ${taskId} in stage ${taskSet.id}"
        val taskLocality = TaskLocality.PROCESS_LOCAL
        val execId = taskSchedule.get(0).toString
        val execHost = executorIdToHost(execId)
        val attemptNum = 0

        logInfo("[HCS] Preparing execution of task " + taskId + " at " + System.currentTimeMillis())
        var taskInfo = new TaskInfo(taskId,
          index,
          attemptNum,
          System.currentTimeMillis(), // DON'T TRUST THIS VALUE! IT'S BEING SET TOO EARLY
          execId,
          execHost,
          taskLocality,
          false)

        taskSetManager.taskInfos.synchronized {
          val prev = taskSetManager.taskInfos.size
          taskSetManager.taskInfos(taskId) = taskInfo
          logInfo("[HCS] Adding task info for task " + taskId + " (taskSetManager=" + taskSetManager.toString + " taskInfos.size=" + prev + "->" + taskSetManager.taskInfos.size + ")" + " " + Thread.currentThread().getId)
        }

        // Serialize and return the task
        val serializedTask: ByteBuffer = try {
          ser.serialize(task)
        } catch {
          // If the task cannot be serialized, then there's no point to re-attempt the task,
          // as it will always fail. So just abort the whole task-set.
          case NonFatal(e) =>
            val msg = s"Failed to serialize task $taskId, not attempting to retry it."
            logError(msg, e)
            throw new TaskNotSerializableException(e)
        }
        if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024) {
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
        }

        val taskDescription = new TaskDescription(
          taskId,
          attemptNum,
          execId,
          taskInfo.launchTime,
          taskName,
          index,
          addedFiles,
          addedJars,
          task.localProperties,
          serializedTask)

        logInfo(s"Starting $taskName (TID $taskId, $execHost, executor ${taskInfo.executorId}, " +
          s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit} bytes)")

        taskSetManager.taskInfos(taskId) = taskInfo
        logInfo("[HCS] Launching " + taskDescription.name)

        val etq = executorTaskQueue.synchronized {
          executorTaskQueue.getOrElseUpdate(execId, new mutable.Queue[Tuple4[Task[_],
            TaskInfo, TaskDescription, TaskSetManager]]()) += Tuple4(task, taskInfo,
            taskDescription, taskSetManager)
        }

        logInfo("[HCS] There are " + etq.size + " pending tasks for executor " +
          execId + " (" + executorLoad.getOrElse(execId, new AtomicInteger(0)) + "/" +
          executorCores.getOrElse(execId, -1) + ")")

        etq.synchronized {
          while (executorLoad.getOrElseUpdate(execId, new AtomicInteger(0)).get
            < executorCores(execId) && !etq.isEmpty) {
            val t = etq.dequeue
            sc.dagScheduler.taskStarted(t._1, t._2)
            taskLauncher.launchTask(t._3)
            taskScheduler.taskLaunched(t._2.taskId, t._2, t._4)
            executorLoad(execId).incrementAndGet()
            startedTaskIds += (t._2.taskId -> t._2)
          }
        }
      }
    }

    startedTaskIds
  }

  def submitStage(taskSetManager: TaskSetManager): Unit = {
    // Future[HashMap[Long, TaskInfo]] = {
    val start = System.currentTimeMillis()
    val (schedule, futureUrl) = getStageSchedule(sc.applicationId, taskSetManager.stageId)

    if (schedule.isEmpty && !futureUrl.isEmpty) {
      val myThread = new Thread {
        override def run {
          val start = System.currentTimeMillis()
          val schedule = getStageSchedule(futureUrl)
          val end = System.currentTimeMillis()
          logInfo("[HCS] Schedule for stage " + taskSetManager.stageId + " is ready (delay " + (end - start)
            + " ms)")
          executeStage(taskSetManager, schedule)
        }
      }

      myThread.start()
      logInfo("[HCS] Started thread to wait for schedule of stage " + taskSetManager.stageId)
    } else {
      val end = System.currentTimeMillis()
      logInfo("[HCS] Schedule for stage " + taskSetManager.stageId + " is ready (delay " + (end - start) + " ms)")
      executeStage(taskSetManager, schedule)
    }
  }

  /** Event listener */
  override def onApplicationStart(event: SparkListenerApplicationStart) {
    logInfo("[HCS] Application " + event.appId
      + " (" + event.appName + ") started event")

    val url = new URL(baseURL + "/apps")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val json = ("id" -> event.appId) ~
      ("name" -> event.appName) ~
      ("timestamp" -> event.time * 1000) ~
      ("event" -> "start")
    val jsonStr = compact(render(json))

    post(conn, jsonStr)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd) {
    logInfo("[HCS] Application " + sc.applicationId
      + " (" + sc.appName + ") end event")
    val url = new URL(baseURL + "/apps")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val json = ("id" -> sc.applicationId) ~
      ("name" -> sc.appName) ~
      ("timestamp" -> event.time * 1000) ~
      ("event" -> "end")
    val jsonStr = compact(render(json))

    post(conn, jsonStr)
  }

  override def onJobStart(event: SparkListenerJobStart) {
    logInfo("[HCS] Job " + event.jobId + " started event")
    var stages = ListBuffer[Tuple7[Int, Seq[Int], Int, String, String, Int, Array[mutable.HashMap[String, Long]]]]()

    for (stageInfo <- event.stageInfos) {
      val stageType = "compute"
      var stageKey = stageInfo.key

      /*
        rddInfo.locations contains a map per RDD that tells me how much data is on each node. It's important to
        remember that all RDDs here have the same number of partitions, hence if I want to know how much input data
        is on a node for a task, I simply have to add up the number of bytes per host for a partition index for all
        RDDs. Also remember that the number of tasks in a stage corresponds to the number of partitions in its input
        data.
       */

      val locationsPerTask = Array.fill[mutable.HashMap[String, Long]](stageInfo.numTasks)(mutable.HashMap[String, Long]())
      for (rddInfo <- stageInfo.rddInfos) {
        for (pidx <- 0 until rddInfo.locations.size) {
          for (loc <- rddInfo.locations(pidx)) {
            locationsPerTask(pidx).getOrElseUpdate(loc._1, 0L)
            locationsPerTask(pidx)(loc._1) += loc._2
          }
        }
      }

      stages += Tuple7(stageInfo.stageId,
        stageInfo.parentIds,
        stageInfo.numTasks,
        stageType,
        stageInfo.name,
        stageKey,
        locationsPerTask)

      for (rddInfo <- stageInfo.rddInfos) {
        logInfo("[HCS] stage " + stageInfo.stageId + " rdd " + rddInfo.id + " ("
          + rddInfo.callSite + ") has " + rddInfo.numPartitions
          + " partitions.")
      }

      logInfo("[HCS] stage " + stageInfo.stageId + " input data locations [" +
        locationsPerTask.zipWithIndex.map(entry => entry._2 + " -> ["
          + locationsPerTask(entry._2).map(l => l._1 + "->" + l._2).mkString(", ")
          + "]").mkString(", ")
        + "]")
    }

    val json = ("id" -> event.jobId) ~
      ("timestamp" -> event.time * 1000) ~
      ("event" -> "start") ~
      ("stages" -> stages.map(s => ("id" -> s._1) ~
        ("parents" -> s._2) ~
        ("size" -> s._3) ~
        ("type" -> s._4) ~
        ("function" -> s._5) ~
        ("key" -> s._6) ~
        ("locations" -> s._7.toSeq)))

    val url = new URL(baseURL + "/apps/" + sc.applicationId + "/jobs")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    val jsonStr = compact(render(json))

    post(conn, jsonStr)
  }

  override def onJobEnd(event: SparkListenerJobEnd) {
    logInfo("[HCS] Job " + event.jobId + " finished event")

    val url = new URL(baseURL + "/apps/" + sc.applicationId + "/jobs")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    val json = ("id" -> event.jobId) ~
      ("timestamp" -> event.time * 1000) ~
      ("event" -> "end")
    val jsonStr = compact(render(json))

    post(conn, jsonStr)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted) {
    logInfo("[HCS] Stage " + event.stageInfo.stageId + " submitted event (ignored)")
  }

  override def onStageCompleted(event: SparkListenerStageCompleted) {
    logInfo("[HCS] Stage " + event.stageInfo.stageId + " completed event (ignored)")
  }

  override def onTaskStart(event: SparkListenerTaskStart) {
    logInfo("[HCS] Task " + event.stageId + "/" + event.taskInfo.id + " started event at " + clock.getTimeMillis() +
      " ms")

    val url = new URL(baseURL + "/apps/" + sc.applicationId + "/jobs/" + event.taskInfo.jobId
      + "/stages/" + event.stageId + "/tasks/" + event.taskInfo.index)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val json = ("index" -> event.taskInfo.index) ~
      ("stage" -> event.stageId) ~
      ("application" -> sc.applicationId) ~
      ("event" -> "start") ~
      ("timestamp" -> clock.getTimeMillis() * 1000)
    //("timestamp" -> event.taskInfo.launchTime * 1000)
    val jsonStr = compact(render(json))

    patch(conn, jsonStr)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd) {
    logInfo("[HCS] Task " + event.stageId + "/" + event.taskInfo.id + " end event at " + event.taskInfo.finishTime +
      " ms")

    val url = new URL(baseURL + "/apps/" + sc.applicationId + "/jobs/" + event.taskInfo.jobId
      + "/stages/" + event.stageId + "/tasks/" + event.taskInfo.index)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val json = ("index" -> event.taskInfo.index) ~
      ("stage" -> event.stageId) ~
      ("application" -> sc.applicationId) ~
      ("event" -> "end") ~
      ("bytesRead" -> event.taskMetrics.inputMetrics.bytesRead) ~
      ("bytesWritten" -> event.taskMetrics.outputMetrics.bytesWritten) ~
      ("gbcTime" -> event.taskMetrics.jvmGCTime * 1000) ~ // ms -> us
      ("cpuTime" -> event.taskMetrics.executorCpuTime / 1000) ~ // ns -> us
      ("timestamp" -> event.taskInfo.finishTime * 1000) // ms -> us
    val jsonStr = compact(render(json))


    patch(conn, jsonStr)

    val execId = event.taskInfo.executorId
    val etq = executorTaskQueue.synchronized {
      executorTaskQueue.get(execId)
    }.get

    executorLoad(execId).decrementAndGet()

    logInfo("[HCS] There are " + etq.size + " pending tasks for executor " +
      execId + " (" + executorLoad.get(execId) + "/" + executorCores(execId) + ")")

    etq.synchronized {
      while (executorLoad(execId).get < executorCores(execId) && !etq.isEmpty) {
        val t = etq.dequeue
        logInfo("[HCS] t=" + t.toString() + " sc=" + sc.toString)
        logInfo("[HCS] sc.dagScheduler=" + sc.dagScheduler.toString)
        sc.dagScheduler.taskStarted(t._1, t._2)
        taskLauncher.launchTask(t._3)
        taskScheduler.taskLaunched(t._2.taskId, t._2, t._4)
        executorLoad(execId).incrementAndGet()
      }
    }
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded) {
    logInfo("[HCS] Executor " + event.executorInfo.executorHost
      + "/" + event.executorId + " with "
      + event.executorInfo.totalCores + " added event")

    executorCores += (event.executorId -> event.executorInfo.totalCores)
    executorLoad += (event.executorId -> new AtomicInteger(0))
    executorIdToHost += (event.executorId -> event.executorInfo.executorHost)

    sc.taskScheduler.asInstanceOf[TaskSchedulerImpl].registerExecutor(event.executorId, event.executorInfo.executorHost)

    val url = new URL(baseURL + "/executors")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val json = ("id" -> event.executorId) ~
      ("cores" -> event.executorInfo.totalCores) ~
      ("host" -> event.executorInfo.executorHost) ~
      ("application" -> sc.applicationId)

    val jsonStr = compact(render(json))

    post(conn, jsonStr)
    conn.disconnect()
  }

  def getStageSchedule(appId: String, stageId: Int):
  Tuple2[Map[String, Array[Long]], String] = {

    logInfo("[HCS] Getting schedule for application " + appId + " stage " + stageId)

    val url = new URL(baseURL + "/apps/" + appId + "/schedules/" + stageId)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val resp = get(conn)
    val rc = conn.getResponseCode

    rc match {
      case 200 =>
        val exec = parse(resp).extract[Option[Map[String, Array[Long]]]]
        if (exec.isEmpty) {
          Tuple2(new HashMap[String, Array[Long]]().toMap, "")
        } else {
          Tuple2(exec.get, "")
        }

      case 202 =>
        logInfo("[HCS] Schedule for stage " + stageId + " will be available under " + conn.getHeaderField("Location"))

        Tuple2(new HashMap[String, Array[Long]]().toMap, conn.getHeaderField("Location"))
    }
  }

  def getStageSchedule(futureUrl: String): Map[String, Array[Long]] = {

    logInfo("[HCS] Waiting for schedule on " + futureUrl)

    val url = new URL(baseURL + futureUrl)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]

    val resp = get(conn)
    val rc = conn.getResponseCode

    rc match {
      case 200 =>
        val exec = parse(resp).extract[Option[Map[String, Array[Long]]]]
        exec.get
    }
  }

  private def post(conn: HttpURLConnection, data: String): Unit = {
    val t0 = System.currentTimeMillis()

    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)

    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.close()
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        logError("Could not open connection to " + baseURL)
    }

    val t1 = System.currentTimeMillis()
    logInfo("[HCS] Server needed " + (t1 - t0) + "ms to respond to " + conn.getURL)

    readResponse(conn)
  }

  private def patch(conn: HttpURLConnection, data: String): Unit = {
    val t0 = System.currentTimeMillis()

    // java doesn't seem to support the PATCH method, so I have to use this workaround
    conn.setRequestMethod("POST")
    conn.setRequestProperty("X-HTTP-Method-Override", "PATCH")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)

    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.close()
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        logError("Could not open connection to " + baseURL)
    }

    val t1 = System.currentTimeMillis()
    logInfo("[HCS] Server needed " + (t1 - t0) + "ms to respond to " + conn.getURL)

    readResponse(conn)
  }

  private def get(conn: HttpURLConnection): String = {
    val t0 = System.currentTimeMillis()

    conn.setRequestMethod("GET")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(false)

    val t1 = System.currentTimeMillis()
    logInfo("[HCS] Server needed " + (t1 - t0) + "ms to respond to " + conn.getURL)

    readResponse(conn)
  }

  def readResponse(conn: HttpURLConnection): String = {
    val dataStream = conn.getInputStream
    val responseJson = Source.fromInputStream(dataStream).mkString
    logInfo("[HCS] Response from server: " + responseJson)
    responseJson
  }
}


private[spark] object HcsSchedulerClient {

}
