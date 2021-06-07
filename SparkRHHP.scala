import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object SparkRHHP {
  def getTuple(x:String):ArrayBuffer[(String,(Double,Double))]={
    val arr = x.split(",")
    val res = new ArrayBuffer[(String, (Double,Double))]()
    for(i <- 0 until arr.length-1){
      res.append((i.toString+"-"+arr(arr.length-1),(arr(i).toDouble,arr(i).toDouble)))
    }
    res
  }
  //
  def getHMatrix(x:String,bLUmap:Broadcast[Map[Int,ArrayBuffer[(Double,Double,String)]]]):Map[Int,ArrayBuffer[String]]={
    val LUmap = bLUmap.value
    val arr = x.split(",")
    var res = Map[Int,ArrayBuffer[String]]()
    for(i<- 0 until arr.length-1){
      for(a<-LUmap(i)){
        if (a._1 <= arr(i).toDouble &&  arr(i).toDouble <= a._2){
          if (res.keySet.contains(i)){
            res(i).append(a._3)
          }
          else {
            res += (i->ArrayBuffer(a._3))
          }
        }
      }
    }
    res
  }

  def getRelevance(x:Map[Int,ArrayBuffer[String]]):ArrayBuffer[(Int,Int)]={
    val res = new ArrayBuffer[(Int,Int)]()
    for(i <- x.keySet){
      res.append((i,1-Math.min(1,x(i).size-1)))
    }
    res
  }

  def getDepen(x:Map[Int,ArrayBuffer[String]], bSset:Broadcast[ArrayBuffer[Int]],
               bCset:Broadcast[mutable.Set[Int]]):ArrayBuffer[(Int,Int)] ={
    val Sset = bSset.value
    val Cset = bCset.value
    val res = new ArrayBuffer[(Int, Int)]()
    var tem = x(Sset(0)).clone()
    for(i <- Sset.indices){
      tem = tem.intersect(x(Sset(i)))
    }
    for(key <- Cset){
      val tem1 = tem.intersect(x(key))
      res.append((key, 1-Math.min(1,tem1.size-1)))
    }
    res
  }

  def getSignf(x:Map[Int,ArrayBuffer[String]],bSset:Broadcast[ArrayBuffer[Int]],
               bCset:Broadcast[mutable.Set[Int]]):ArrayBuffer[(Int,Int)]={
    val Sset = bSset.value
    val Cset = bCset.value
    val res = new ArrayBuffer[(Int, Int)]()
    for(key <- Cset){
      var sum = 0
      for (a <- Sset){
        val Va = 1-Math.min(1,x(a).size-1)
        val Vakey = 1-Math.min(1, x(a).intersect(x(key)).size-1)
        sum = sum + (Vakey - Va)
      }
      res.append((key, sum))
    }
    res
  }
  def main(args: Array[String]): Unit = {
    println("input the filepath:")
    val filepath = StdIn.readLine()
    println("input the number of attrs:")
    val Attrnum = StdIn.readInt()
    println("input the number of instances:")
    val n = StdIn.readInt()
    println("input omega :")
    val omega = StdIn.readDouble()
    println("input Lambda :")
    val Lambda = StdIn.readDouble()
    println("input the number of Partitions:")
    val partitionNum = StdIn.readInt()
    println("input the number of selected attrs:")
    val selectNum = StdIn.readInt()

    val conf = new SparkConf().setAppName("Spark-RH-HP").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val tem = new ArrayBuffer[Long]()
        val t1 = System.currentTimeMillis()
        val LU1 = sc.textFile(filepath,partitionNum).flatMap(x=>getTuple(x))
          .reduceByKey((x1, x2)=>(Math.min(x1._1,x2._1),Math.max(x1._2,x2._2)),partitionNum)
          .map(x=>(x._1.split("-")(0).toInt,(x._2._1,x._2._2,x._1.split("-")(1)))).collect()
        var LUmap = Map[Int,ArrayBuffer[(Double,Double,String)]]()
        LU1.foreach(x=>{
          if(LUmap.keySet.contains(x._1)){
            LUmap(x._1).append(x._2)
          }
          else {
            LUmap += (x._1->ArrayBuffer(x._2))
          }
        })
    val bLUmap = sc.broadcast(LUmap)
        val HMatrix = sc.textFile(filepath,partitionNum).map(x=>getHMatrix(x,bLUmap)).persist(StorageLevel.MEMORY_AND_DISK)
        val relevance = HMatrix.flatMap(x=>getRelevance(x)).reduceByKey(_+_).collect()
          .map(x=>(x._1,x._2.toDouble)).toMap
        val Cset: mutable.Set[Int] = mutable.Set()
        for(i <- 0 until Attrnum){
          Cset.add(i)
        }
    val Sset = new ArrayBuffer[Int]()
    val maxRel = relevance.maxBy(_._2)
        var lastDepen = maxRel._2
        Sset.append(maxRel._1)
        Cset.remove(maxRel._1)
        tem.append(System.currentTimeMillis()-t1)
        var flag = true
        while (Sset.size<selectNum && Cset.nonEmpty){
          val bSset = sc.broadcast(Sset)
          val bCset = sc.broadcast(Cset)
          val depen = HMatrix.flatMap(x=>getDepen(x,bSset,bCset)).reduceByKey(_+_).collect()
            .map(x=>(x._1,x._2.toDouble)).toMap
          depen.foreach(x=>{
            if (x._2 == lastDepen){
              Cset.remove(x._1)
            }
          })
          val signf = HMatrix.flatMap(x=>getSignf(x,bSset,bCset)).reduceByKey(_+_).collect()
            .map(x=>(x._1,x._2.toDouble)).toMap

          var bestA = -1
          var bestV = 0.0
          var temLast = 0.0
          for(a <- Cset){
            val v = omega*relevance(a) /n.toDouble + Lambda*(1.0-omega)*(depen(a)-lastDepen)/n.toDouble + (1.0-omega)*(1.0-Lambda)*signf(a)/n.toDouble/Sset.size.toDouble
            if(v > bestV ||(v == bestV && a < bestA)){
              bestA = a
              bestV = v
              temLast = depen(a)
            }
          }
          if(bestA == -1){
            flag =false
          }
          else {
            Sset.append(bestA)
            Cset.remove(bestA)
            lastDepen = temLast
            tem.append(System.currentTimeMillis()-t1)
          }
          println(Sset.mkString(","))
        }
    println(tem.mkString("\t"))
  }
}
