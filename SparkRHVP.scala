import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object SparkRHVP {
  def getTuple(x:String):ArrayBuffer[(String,(Double,Double))]={
    val arr = x.split(",")
    val res = new ArrayBuffer[(String, (Double,Double))]()
    for(i <- 0 until arr.length-1){
      res.append((i.toString+"-"+arr(arr.length-1),(arr(i).toDouble,arr(i).toDouble)))
    }
    res
  }
  def colTransform(x:Iterator[String], bLU:Broadcast[Map[Int,ArrayBuffer[(Double,Double,String)]]] )
  :Map[Int,ArrayBuffer[ArrayBuffer[String]]]={
    val LU = bLU.value
    var res = Map[Int, ArrayBuffer[ArrayBuffer[String]]]()
    while (x.hasNext){
      val arr = x.next().split(",")
      for(i <- 0 until arr.length-1){
        val tem = new ArrayBuffer[String]()
        for(luarr <- LU(i)){
          if(luarr._1<=arr(i).toDouble && luarr._2>=arr(i).toDouble){
            tem.append(luarr._3)
          }
        }
        if(!res.keySet.contains(i)){
          res += (i->ArrayBuffer(tem))
        }
        else {
          res(i).append(tem)
        }
      }
    }
    res
  }
  //


  def getHMatrix(x:(Int,ArrayBuffer[(Int,ArrayBuffer[(Double,String)])]))
  :ArrayBuffer[(Int,(Int,ArrayBuffer[ArrayBuffer[String]]))]={
    var LU = Map[String,(Double,Double)]()
    for(arr <- x._2){
      for(a <- arr._2){
        if(LU.keySet.contains(a._2)){
          LU += (a._2->(Math.min(LU(a._2)._1,a._1),Math.max(LU(a._2)._2,a._1)))
        }
        else {
          LU += (a._2->(a._1,a._1))
        }
      }
    }

    val res = new ArrayBuffer[(Int,(Int,ArrayBuffer[ArrayBuffer[String]]))]()
    for(arr <- x._2){
      val res3 = ArrayBuffer[ArrayBuffer[String]]()
      for(a <- arr._2){
        val res4 = ArrayBuffer[String]()
        for(lu <- LU){
          if (lu._2._1 <= a._1 &&  a._1 <= lu._2._2){
            res4.append(lu._1)
          }
        }
        res3.append(res4)
      }
      res.append((x._1,(arr._1,res3)))
    }
    res
  }


  def getDepenAndSignf(x:Iterator[(Int,(ArrayBuffer[ArrayBuffer[String]],ArrayBuffer[ArrayBuffer[String]]),(Double,Double,Double))],
                       bbest:Broadcast[Int])
  :Iterator[(Int,(ArrayBuffer[ArrayBuffer[String]],ArrayBuffer[ArrayBuffer[String]]),(Double,Double,Double))]={
    val best = bbest.value
    val arr = x.toArray
    var bestIns:(Int,(ArrayBuffer[ArrayBuffer[String]],ArrayBuffer[ArrayBuffer[String]]),(Double,Double,Double)) = null
    for(tem <- arr){
      if(tem._1 == best){
        bestIns = tem
      }
    }

    for(a <- arr.indices){
      var rel = 0.0
      var signf = 0.0
      if(arr(a)._1!=best){
        val depenArr = new ArrayBuffer[ArrayBuffer[String]]()
        for(i <- arr(a)._2._1.indices){
          val tem = arr(a)._2._1(i).intersect(bestIns._2._2(i))
          depenArr.append(tem)
          if(tem.size==1){
            rel += 1.0
          }

          if(arr(a)._2._1(i).intersect(bestIns._2._1(i)).size==1){
            signf += 1.0
          }

        }
        signf = signf - bestIns._3._1 + arr(a)._3._3
        arr(a) = (arr(a)._1,(arr(a)._2._1,depenArr),(arr(a)._3._1, rel ,signf))
      }
    }
    arr.iterator
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

    val conf = new SparkConf().setAppName("Spark-RH-VP").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val fileRdd = sc.textFile(filepath,partitionNum).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val LUmap = fileRdd
      .flatMap(x=>getTuple(x))
      .reduceByKey((x1, x2)=>(Math.min(x1._1,x2._1),Math.max(x1._2,x2._2)))
      .map(x=>(x._1.split("-")(0).toInt,ArrayBuffer((x._2._1,x._2._2,x._1.split("-")(1)))))
      .reduceByKey(_++_)
      .collect().toMap
    val bLUmap = sc.broadcast(LUmap)
    var HHmatrixRdd = fileRdd
      .mapPartitions {iter => colTransform(iter, bLUmap).iterator }
      .map(x => {
        var count = 0.0
        for (a <- x._2) {
          if (a.size == 1) {
            count += 1.0
          }
        }
        (x._1, (x._2,x._2), (count, count, 0.0))
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    fileRdd.unpersist(true)
    val rel = HHmatrixRdd.map(x=>(x._1,x._3._1)).reduceByKey(_+_)
      .reduce((x,y)=>{
        if (y._2 > x._2){
          y
        }
        else {
          if(y._2 == x._2 && y._1 < x._1){
            y
          }
          else{
            x
          }
        }

      })

    val Cset: mutable.Set[Int] = mutable.Set()
    for(i <- 0 until Attrnum){
      Cset.add(i)
    }
    val Sset = new ArrayBuffer[Int]()
    Sset.append(rel._1)
    Cset.remove(rel._1)
    var lastDepen = rel._2
    while (Sset.size<selectNum && Cset.nonEmpty){
      val best = Sset(Sset.size-1)
      val bbest = sc.broadcast(best)
      val depen = HHmatrixRdd
        .filter(x=>{
          Cset.contains(x._1) || x._1 == bbest.value
        })
        .mapPartitions(x=>getDepenAndSignf(x,bbest))
        .persist(StorageLevel.DISK_ONLY)
      val index = depen
        .map(x=>(x._1,(x._3._1,x._3._2,x._3._3)))
        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      index.filter(x=>{
        x._2._2==lastDepen}).map(_._1).collect()
        .foreach(x=>{
        Cset.remove(x)
        })
      HHmatrixRdd.unpersist()
      if(Cset.nonEmpty) {
        val bestA = index.filter(x=>{Cset.contains(x._1)})
          .map(x => {
            (x._1,x._2._2,
              (omega * x._2._1.toDouble/ n.toDouble) + ((Lambda * (1.0 - omega)) * (x._2._2-lastDepen) / n.toDouble) + ((1.0 - omega) * (1.0 - Lambda) * x._2._3 / Sset.size.toDouble/ n.toDouble))
          })
          .reduce((x, y) => {
            if (y._3 > x._3 || (y._3 == x._3 && y._1 < x._1)) {
              y
            }
            else {
                x
            }
          })
        Sset.append(bestA._1)
        Cset.remove(bestA._1)
        lastDepen = bestA._2
      }
      index.unpersist()
      HHmatrixRdd = depen
    }
    println("result:")
    println(Sset.mkString(","))
  }
}
