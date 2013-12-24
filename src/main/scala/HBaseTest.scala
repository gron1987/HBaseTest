package main.scala

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import java.security._
import scala.collection.JavaConverters._
import java.util
import javax.xml.bind.annotation.adapters.HexBinaryAdapter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.filter.{KeyOnlyFilter, FirstKeyOnlyFilter}
import org.apache.hadoop.hbase.mapreduce.{TableMapper}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object HBaseTest {

  def insert(table: HTable){
        val md = MessageDigest.getInstance("MD5")
        val l = new util.ArrayList[Put]()
    //    for(d <- 1 to 30){
          val d = 1
          val day = if(d < 10){
            "0" + d.toString
          } else {
            d.toString
          }
          val dateStr = "2013-10-" + day + " 00:00:00"

          for(t <- 1 to 100000){
            var start = System.nanoTime();
            val hex = (new HexBinaryAdapter()).marshal(md.digest(Bytes.toBytes(t)))
            val theput= new Put(Bytes.toBytes(dateStr + " month " + hex))
    //        for(r <- 1 to 20; c <- 1 to 20){
    //          theput.add(Bytes.toBytes("cf"),Bytes.toBytes(r.toString+"r"+c.toString+"c"),Bytes.toBytes(dateStr))
              theput.add(Bytes.toBytes("cf"),Bytes.toBytes("r1c1"),Bytes.toBytes("2013-10-"+d.toString+" 00:00:00"))
              l.add(theput)
    //        }
            table.put(theput)
            l.clear()
            if(t % 1000 == 0){
              println("Day: " + d.toString + " " + t.toString+" token Added ")
              println("Time to add "  + (System.nanoTime() - start))
            }
          }
          println(d.toString+" day Added ")
    //    }
  }

  def count(table: HTable){
    def cnt(r: ResultScanner,size: Int,count: Int): Int = {
      val res = r.next(size)
      print(".")
      if(res.size <= 0) return count
      else return cnt(r,size,count+res.size)
    }

    val nextSizes = List(1000,10000,100000)
    for(size  <- nextSizes){
      val scan = new Scan(Bytes.toBytes("2013-10-01 00:00:00 month"))
      scan.setFilter(new FirstKeyOnlyFilter())
      scan.setCaching(5000)
      val result=table.getScanner(scan)
      println("Size " + size.toString)
      println("")
      val start = System.nanoTime();
      val count = cnt(result,size,0)
      println("")
      println("Time to SELECT ALL "  + (System.nanoTime() - start))
      println("Count : " + count.toString)
    }
  }

  def main(args: Array[String]){
    val customConf = new Configuration()
    customConf.setLong("hbase.client.scanner.caching", 3000)
    val conf = HBaseConfiguration.create(customConf)
    conf.set("hbase.zookeeper.quorum", "zk1.smartdata.cloudmade.com,zk2.smartdata.cloudmade.com,zk3.smartdata.cloudmade.com")

    println("pre")
    val table = new HTable(conf, "mytable")
    println("post")

//    insert(table)

//    count(table)

    val start = System.nanoTime();
    val res = RowCounter.run(conf)
    println("Time to SELECT ALL "  + (System.nanoTime() - start))
    println("Count : " + res.toString)

    println("End")
  }
}
