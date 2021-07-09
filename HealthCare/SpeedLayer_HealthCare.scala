package cts.analytics

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object SpeedLayer_HealthCare {

/*
while read line; do echo -e "$line\n"; sleep .1; done < symptomfeed.csv | nc -lk 9991

spark-submit --class "cts.analytics.SpeedLayer_HealthCare" --master local[*] SpeedLayer_HealthCare.jar 0 localhost 9991 5 60 10

========================================

while read line; do echo -e "$line\n"; sleep .1; done < sensorfeed.csv | nc -lk 9991

spark-submit --class "cts.analytics.SpeedLayer_HealthCare" --master local[*] SpeedLayer_HealthCare.jar 1 localhost 9991 5
*/

  def generic_mapper(line:String) = {
    val fields = line.split(',')
    if(fields.length == 2){(fields(1),fields(0))}
    else{("NA","NA")}
  }
  
  def generic_mapper2(line:String) = {
    val fields = line.split(',')
    if(fields.length == 7){
      if(!fields(0).startsWith("PATIENT")){
      (fields(0),fields(3).toInt,fields(4).toInt,fields(5).toInt,fields(6).toInt)}else{("NA",0,0,0,0)}
      }
    else{("NA",0,0,0,0)}
  }  

  case class Sensor(Patient:String, Temp:Int, HeartRate:Int, SYS:Int, DIA:Int)

  def model_mapper(T:(String,Int,Int,Int,Int)): Sensor = {
    val sensor:Sensor = Sensor(T._1,T._2,T._3,T._4,T._5)
        
    return sensor
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
  
    val sparkConf = new SparkConf().setAppName("cts.analytics.HealthCare.SpeedLayer")
    val sc = new StreamingContext(sparkConf, Seconds(args(3).toInt))
    sc.checkpoint("/tmp/HealthCare");
        
    val datastream = sc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)     

    if(args(0).toInt == 1)
    {
      val takerelevantdata =  
        datastream.map(generic_mapper2).
        filter(t => (t._1 != "NA")).
        map(model_mapper)
      //take master from hive  
      val RequiredDStream = takerelevantdata.transform { rdd =>
        {
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
          import spark.implicits._ 
  
          val SensorInferenceDF = rdd.toDF()
          SensorInferenceDF.createOrReplaceTempView("SensorInference")
    
          val SensorInferenceAvg =
            spark.sql("""
              select * 
              from
              (
                select Patient,
                avg(Temp) as Temp,avg(HeartRate) as HeartRate,
                avg(SYS) as SYS,avg(DIA) as DIA
                from SensorInference
                group by Patient
              )obj   
              """)
                  
          SensorInferenceAvg.rdd.map(r => {
            val Temp = r.getAs("Temp").toString().toDouble
            val HeartRate = r.getAs("HeartRate").toString().toDouble
            val SYS = r.getAs("SYS").toString().toDouble
            val DIA = r.getAs("DIA").toString().toDouble
            var Alert = "Normal"
           
            if(Temp >=35 && Temp <=97 && HeartRate >=60 && HeartRate <=100 && SYS >=90 && SYS <=120 && DIA >=60 && DIA <=80){Alert = "Normal"}
            if(Temp >=35 && Temp <=97 && HeartRate > 100 && SYS >=90 && SYS <=120 && DIA >=60 && DIA <=80){Alert = "Breathing Difficulty"}
            if(Temp >=35 && Temp <=97 && HeartRate >=60 && HeartRate <=100 && SYS < 90 && DIA < 60){Alert = "Low BP"}
            if(Temp >=35 && Temp <=97 && HeartRate >=60 && HeartRate <=100 && SYS > 130 && DIA >= 80){Alert = "High BP"}
            if((Temp < 35 || Temp > 97) && HeartRate >=60 && HeartRate <=100 && SYS >=90 && SYS <=120 && DIA >=60 && DIA <=80){Alert = "Temperature"}

            (r.getAs("Patient").toString(),Alert)
          })
        }        
      }

      RequiredDStream.foreachRDD 
      { 
        (rdd, time) =>
          if(!rdd.isEmpty()){
            val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
            import spark.implicits._
      
            val SensorAlertDF = rdd.toDF("Patient","Condition")
            
            println(s"=== Sensor Alert === [ $time ] === [ Batch Size : "+args(3)+" Seconds ] ===")
            
            SensorAlertDF.select(SensorAlertDF("Patient"),SensorAlertDF("Condition")).show(false)
        }
      }      
    } 
    
    if(args(0).toInt == 0)
    {    
      val Emergency = sc.sparkContext.parallelize(
          List(
                ("YYYYYNNNNNNYNNN","Covid"),
                ("NNNNNNNNYNNYNYY","Diabetes"),
                ("NNNNNNNNYNNNYNN","Low BP"),
                ("NYNYNNNNNYNYNNN","Cold"),
                ("NNYNNNNNNYNYNNN","Flu"),
                ("NNNNYNNYNNNNYNN","Anaemia"),
                ("NYYNNNNNNNNNNNN","Malaria"),
                ("NNNNNYNYNNNNYNN","Food Poisoning"),
                ("NNYYNNNYNNNYYNN","Typhoid")
                //,("NNNYNNNNNNNYNNN","General")
              )
      )

      val mapdatatopair =  datastream.map(generic_mapper).filter(t => (t._1 != "NA")).filter(t => (!t._2.startsWith("PATIENT")))  
      val RequiredDStream = mapdatatopair.transform { rdd =>
        {            
          rdd.leftOuterJoin(Emergency).map(t => 
            {
              if(t._2._2 == None){("General",1)}
              else{(t._2._2.get,1)}
            }
          )
        }
      }

      val EmergencyWindow =
          		RequiredDStream.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),(a:Int,b:Int) => (a - b),
          		    Durations.seconds(args(4).toInt), Durations.seconds(args(5).toInt))
      EmergencyWindow.foreachRDD 
      { 
        (rdd, time) =>
          if(!rdd.isEmpty()){        
            val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
            import spark.implicits._
      
            val EmergencyDF = rdd.toDF("PrincipalDiagnosis","Admissions")
            EmergencyDF.createOrReplaceTempView("Emergency")
      
            val EmergencyCases =
              spark.sql("select * from Emergency order by Admissions desc")
                
            println(s"=== Emergency Cases === [ $time ] === [ Window Length : "+args(4)+" Seconds / Slide Interval : "+args(5)+" Seconds ] ===")
            
            EmergencyCases.select(EmergencyCases("PrincipalDiagnosis"),EmergencyCases("Admissions")).show(false)   
        }
      }
      
      def Total(newData: Seq[Int], state: Option[Int]) = {
        val newState = state.getOrElse(0) + newData.sum
        Some(newState)
      }
      val EmergencyCasesFinalDStream = RequiredDStream.updateStateByKey(Total)
      EmergencyCasesFinalDStream.foreachRDD 
      { 
        (rdd, time) =>
          if(!rdd.isEmpty()){        
            val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
            import spark.implicits._
      
            val EmergencyCasesDF = rdd.toDF("PrincipalDiagnosis","Admissions")
            EmergencyCasesDF.createOrReplaceTempView("Emergency")
      
            val TotalEmergencyCases =
              spark.sql("select * from Emergency order by Admissions desc")
                
            println(s"=== Total Cases === [ $time ] ===")
            
            TotalEmergencyCases.select(TotalEmergencyCases("PrincipalDiagnosis"),TotalEmergencyCases("Admissions")).show(false)   
        }
      }      
    }
    
    sc.start()
    sc.awaitTermination()      
  }
}