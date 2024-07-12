package com.turing.scala.flink19.pipeline.demo1

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

//定义所需的常量值
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

//实现欺诈的数据处理逻辑
@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  //Flink 中最基础的状态类型是 ValueState，这是一种能够为被其封装的变量添加容错能力的类型
  @transient private var flagState: ValueState[java.lang.Boolean] = _

  // 设置检测时间
  @transient private var timeState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    // ValueState 需要使用 ValueStateDescriptor 来创建，ValueStateDescriptor
    // 包含了 Flink 如何管理变量的一些元数据信息。状态在使用之前需要先被注册。 状态需要使用 open() 函数来注册状态。
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)

    val timeDescriptor = new ValueStateDescriptor("time", Types.LONG)

    // 获取 flagDescriptor 的状态
    flagState = getRuntimeContext.getState(flagDescriptor)

    timeState = getRuntimeContext.getState(timeDescriptor)


  }

  //编写欺诈行为处理逻辑
  @throws[Exception]
  def processElement( transaction: Transaction,
                      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                      collector: Collector[Alert]): Unit = {
    // 交易金额
    val lastTransactionWasSmall = flagState.value()

    // 如果 lastTransactionWasSmall 有值，说明上一次是小金额的
    if (lastTransactionWasSmall !=null) {
      if (transaction.getAmount >FraudDetector.LARGE_AMOUNT){
        //触发预警条件
        val alert = new Alert
        alert.setId(transaction.getAccountId)
        collector.collect(alert)
      }
      //  flagState.clear()
      cleanUp(context)
    }

    if(transaction.getAmount<FraudDetector.SMALL_AMOUNT) {
      // 出现小金额的情况，更新状态
      flagState.update(true)
      // 设置1分钟之后触发
      val timer=context.timerService().currentProcessingTime()+FraudDetector.ONE_MINUTE
      timeState.update(timer)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                       out: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timeState.clear()
    flagState.clear()
  }

  def  cleanUp( ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context):Unit={
    //删除定时器
    val timer=timeState.value()
    ctx.timerService().deleteEventTimeTimer(timer)
    //清空定时器状态
    timeState.clear()
    flagState.clear()
  }

}
