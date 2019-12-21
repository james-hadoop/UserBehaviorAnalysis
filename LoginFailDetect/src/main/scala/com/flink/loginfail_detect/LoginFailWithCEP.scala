package com.flink.loginfail_detect


import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.cep.{CEP, PatternSelectFunction}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 输入的登录事件的样例类
case class LoginEventCep(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的一场报警信息样例类
case class WarningCep(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)


object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取事件数据，创建简单事件流
    val resource = getClass.getResource("/LoginLog.csv")

    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEventCep(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)

      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEventCep](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEventCep): Long = t.eventTime * 1000L
      })
      .keyBy(_.userId)

//    // 2. 定义匹配模式
//    val loginFailPattern = Pattern.begin("begin")
//      .where(_.eventType == "fail")
//      .next("next")
//      .where(_.eventType == "fail")
//      .within(Time.seconds(2))
//
//    // 3. 在事件流上应用模式，得到一个pattern stream
//    val patternStream = CEP.pattern(loginEventStream[LoginEventCep], loginFailPattern[LoginEventCep])
//
//    // 4. 从pattern stream上应用select function，检出匹配事件序列
//    val loginFailDataStream = patternStream.select(new LoginFailMatch())
//
//    loginFailDataStream.print

    env.execute("login fail with CEP")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEventCep, WarningCep] {
  override def select(map: util.Map[String, util.List[LoginEventCep]]): WarningCep = {
    // 从map中按照名称取出对应的事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    WarningCep(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail in 2 seconds.")
  }
}
