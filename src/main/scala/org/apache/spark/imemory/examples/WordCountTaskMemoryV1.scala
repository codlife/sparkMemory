package org.apache.spark.imemory.examples

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
  * We can use this method to get all task memory using information of the same application
  * this method is directly. we use the application's SparkContext as our sparkContext
  * see org.apache.spark.imemory.examples.WordCountTaskMemoryV2 for the more senior method
  * Created by wjf on 16-9-28.
  */
object WordCountTaskMemoryV1 {
//  val conf = new SparkConf().setMaster("spark://ubuntu:7077").setAppName("wordcount")



 val conf = new SparkConf().setMaster("local").setAppName("wordcount")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    println(sc.env.memoryManager.maxOffHeapStorageMemory)
    println(sc.env.memoryManager.maxOnHeapStorageMemory)
    val data = sc.parallelize(Seq("wang jian fei", "wang si min"))

    val rdd = data.flatMap {
      line =>
        val words = line.split(" ")
        words
    }.map((_, 1)).reduceByKey(_ + _)
    println(sc.env.memoryManager.maxOffHeapStorageMemory)
    println(sc.env.memoryManager.maxOnHeapStorageMemory)
    println(sc.env.memoryManager.storageMemoryUsed)
    //    println(sc.taskScheduler)


    rdd.collect().foreach(println)
    sc.jobProgressListener.stageIdToData.foreach(x => x._2.taskData.foreach(task => task._2.metrics.foreach(println)))
  }


}

