package com.flink

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

//样例类
//用户行为
case class MarketingUserBehavior(userId: String, channel: String, behavior: String, timeStamp: Long)

//
object MarketAnalysis {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //添加自定义数据源
    val source: DataStream[MarketingUserBehavior] =
      env.addSource(new EventSourceFunction()).assignAscendingTimestamps(_.timeStamp)

    source.print("...")

    //执行
    env.execute("app 推广")
  }
}

class EventSourceFunction() extends RichSourceFunction[MarketingUserBehavior] {
  //定义一个flag
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {
      val behavior: Seq[String] = Seq("BUY", "DOWNLOAD", "SCAN", "INSTALL", "UNINSTALL")
      val market: Seq[String] = Seq("JD_APP_STORE", "WEIBO", "MI_MALL", "BAIDU_STORE", "GOOGLE_STORE", "V_MALL")
      val timeStamp: Long = System.currentTimeMillis()
      val userId: String = UUID.randomUUID().toString.replaceAll("_", "")
      val random: Random = new Random()
      ctx.collect(MarketingUserBehavior(userId, market(random.nextInt(market.size)), behavior(random.nextInt(behavior.size)), timeStamp))

      TimeUnit.SECONDS.sleep(1)
      count += 1
    }
  }
}