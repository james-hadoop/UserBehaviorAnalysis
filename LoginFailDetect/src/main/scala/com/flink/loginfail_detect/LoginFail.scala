package com.flink.loginfail_detect

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 输入的登录事件的样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的一场报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")

    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)

      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
      })

    val warningStream = loginEventStream
      .keyBy(_.userId) // 以用户id做分组
      .process(new LoginWarning(2))

    warningStream.print()
    env.execute("login fail detect job")

  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val logFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    val loginFailList = logFailState.get()

    // 判度类型是否fail，只添加fail的事件到状态
    if (i.eventType == "fail") {
      if (loginFailList.iterator().hasNext) {
        context.timerService().registerEventTimeTimer(i.eventTime * 1000L + 2000L)
      }

      logFailState.add(i)
    } else {
      //如果成功，清空状态
      logFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 触发定时器的时候，根据状态李的失败个数决定是否输出报警
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = logFailState.get().iterator()
    while (iter.hasNext) {
      allLoginFails += iter.next()
    }

    // 判断个数
    if (allLoginFails.length >= maxFailTimes) {
      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times."))

    }

    // 清空状态
    logFailState.clear()
  }
}