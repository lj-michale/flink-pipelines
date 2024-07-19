//package com.turing.scala.flink19.pipeline.demo2
//
//import java.sql.Timestamp
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
//import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//import scala.collection.mutable.ListBuffer
//
//// 定义输入样例类
//case class UserBehavior(userId: Long,
//                        itemId: Long,
//                        categoryId: Int,
//                        behavior: String,
//                        timestamp: Long )
//
//// 定义窗口聚合结果样例类
//case class ItemViewCount(itemId: Long,
//                         windowEnd: Long,
//                         count: Long )
//
//
//object HotItems {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    //从文件中读取数据，并转换成样例类
//    val inputStream: DataStream[String] = env.readTextFile("D:\\Mywork\\workspace\\Project_idea\\UserBehaviorAnalysis0903\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
//
//    val dataStream = inputStream.map(data => {
//      val arr: Array[String] = data.split(",")
//      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
//    }).assignAscendingTimestamps(_.timestamp * 1000L)
//
//    // 得到窗口聚合结果
//    val aggStream: DataStream[ItemViewCount] = dataStream
//      .filter(_.behavior == "pv") //只要点击行为
//      .keyBy(_.itemId)
//      .timeWindow(Time.hours(1), Time.minutes(5))
//      .aggregate(new CountAgg(), new ItemViewWindowResult())
//
//    val resultStream = aggStream
//      .keyBy(_.windowEnd) //按照窗口分组，收集当前窗口内的商品count数据
//      .process(new TopNHotItems(10))  //自定义处理流程
//
//    dataStream.print("data")
//    resultStream.print()
//
//    env.execute("hot items")
//  }
//}
//
////==>自定义预聚合函数 [输入, 状态, 输出]  (窗口聚合规则)
//class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
//  override def createAccumulator(): Long = 0L
//
//  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1
//
//  override def getResult(accumulator: Long): Long = accumulator
//
//  override def merge(a: Long, b: Long): Long = a + b // 无用
//}
//
////==>自定义处理流程 [key, I, O]
//class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
//  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//    val itemId = key
//    val windowEnd = window.getEnd
//    val count = input.iterator.next()
//    out.collect(ItemViewCount(itemId, windowEnd, count))
//  }
//}
//
////==>自定义处理流程 [key, I, O]
//class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
//  // 先定义状态：ListState
//  private var itemViewCountListState: ListState[ItemViewCount] = _
//
//  override def open(parameters: Configuration): Unit = {
//    itemViewCountListState = getRuntimeContext
//      .getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
//  }
//
//  // 每来一条数据，需要的操作
//  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
//    // 每来一条数据，直接加入ListState
//    itemViewCountListState.add(i)
//
//    // 注册一个windowEnd + 1毫秒 之后出发定时器
//    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
//  }
//
//  //当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出了
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
//    // 为了方便排序，另外定义一个Listbuffer(可排序)，保存ListState里所有数据
//    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
//    val iter = itemViewCountListState.get().iterator()
//    while (iter.hasNext){
//      allItemViewCounts += iter.next()
//    }
//
//    // 清空状态，节省内存
//    itemViewCountListState.clear()
//
//    // 按照Count大小排序,取top5
//    val sortedItemViewCounts = allItemViewCounts
//      .sortBy(_.count)(Ordering.Long.reverse) //柯里化传隐式参数
//      .take(topSize)
//
//    // 将排名信息格式化String，便于打印输出可视化展示
//    val result: StringBuilder = new StringBuilder
//    result.append("窗口结束时间： ").append(new Timestamp(timestamp - 1)).append("\n")
//
//    // 遍历结果列表中每个ItemViewCount, 输出到一行
//    for (elem <- sortedItemViewCounts.indices) {
//      val currentItemViewCount = sortedItemViewCounts(elem)
//      result.append("NO.").append(elem + 1)
//        .append(" 商品Id = ").append(currentItemViewCount.itemId)
//        .append(" 热门度 = ").append(currentItemViewCount.count).append("\n")
//    }
//
//    result.append("=========================================\n\n")
//    Thread.sleep(1000)
//    out.collect(result.toString())
//  }
//
//}