package cts.analytics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.storage.StorageLevel._

object BatchLayer_Healthcare {
  
val run = """
spark-submit --class "cts.analytics.BatchLayer_Healthcare" --master local[*] --jars hive-hbase-handler-2.1.1.jar \
BatchLayer_Healthcare.jar quickstart.cloudera /user/healthcare/transaction/Appointment/Hot/*/* /user/healthcare/transaction/Prescribes/Hot/*/* \
/user/healthcare/transaction/Medication/Hot/*/* /user/healthcare/transaction/undergoes/Hot/*/* /user/healthcare/transaction/Procedures/Hot/*/* 
"""  
  
  case class appointment(appointmentID: Int, patient: Int, prepnurse:Int ,physicianid:Int ,
      start: java.sql.Timestamp, end: java.sql.Timestamp ,examinationroom: String )

  def appointmentmapper(line: String): appointment = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val fields = line.split(',')
    val appoint: appointment = appointment(fields(0).toInt, fields(1).toInt,
        {if(fields(2) == "null"){0}else{fields(2).toInt}}, 
        fields(3).toInt,
      new java.sql.Timestamp(format.parse(fields(4)).getTime()),new java.sql.Timestamp(format.parse(fields(5)).getTime()),fields(6))
       
    return appoint
  }
  
  case class prescribes(physician: Int, patient: Int, medication:Int, date: java.sql.Timestamp, appointment:Int , dose: String)

  def presmapper(line: String): prescribes = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val fields = line.split(',')
    val pres: prescribes = prescribes(fields(0).toInt, fields(1).toInt,fields(2).toInt,
      new java.sql.Timestamp(format.parse(fields(3)).getTime()),
      {if(fields(4) == "null"){0}else{fields(4).toInt}},
      fields(5))
    return pres
  }
  
  case class medication(code: Int, name: String, brand:String, description:String)

  def medmapper(line: String): medication = {
    val fields = line.split(',')
    val med: medication = medication(fields(0).toInt, fields(1), fields(2),fields(3))
    return med
  }

  case class undergoes( patient: Int,procedure:Int,stay:Int, date: java.sql.Timestamp, physician:Int , assisstingnurse: Int/*,disease:String*/)

  def undergoesmapper(line: String): undergoes = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val fields = line.split(',')
    val under: undergoes = undergoes(fields(0).toInt, fields(1).toInt,fields(2).toInt,
      new java.sql.Timestamp(format.parse(fields(3)).getTime()),fields(4).toInt,fields(5).toInt/*, fields(6)*/)
    return under
  }

  case class procedures(code: Int, name: String, cost:Double)

  def proceduresmapper(line: String): procedures = {
    val fields = line.split(',')
    val procd: procedures = procedures(fields(0).toInt, fields(1), fields(2).toDouble)
    return procd
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val SparkConf = new SparkConf().setAppName("cts.analytics.Healthcare.BatchLayer")
    val spark = SparkSessionSingleton.getInstance(SparkConf)
    import spark.implicits._
   
    val lines = spark.sparkContext.textFile("hdfs://"+args(0)+":8020"+args(1))
    val lines1 = spark.sparkContext.textFile("hdfs://"+args(0)+":8020"+args(2))
    val lines2 = spark.sparkContext.textFile("hdfs://"+args(0)+":8020"+args(3))
    val lines3 = spark.sparkContext.textFile("hdfs://"+args(0)+":8020"+args(4))
    val lines4 = spark.sparkContext.textFile("hdfs://"+args(0)+":8020"+args(5))
    /*val cc = lines.toDF()
    cc.show(false)
    val cc1 = lines1.toDF()
    cc1.show(false)
    val cc2 = lines2.toDF()
    cc2.show(false)
    val cc3 = lines3.toDF()
    cc3.show(false)
    val cc4 = lines4.toDF()
    cc4.show(false) */   
    val appointment = lines.map(appointmentmapper)
    appointment.cache
  
    val Appointment = appointment.toDF()
    //Appointment.show()
    Appointment.createOrReplaceTempView("appointment");
  
    val prescribes= lines1.map(presmapper)
    prescribes.cache
    
    val Prescribes = prescribes.toDF()
    //Prescribes.show()
    Prescribes.createOrReplaceTempView("prescribes");  
    
    val medication = lines2.map(medmapper)
    medication.cache
  
    val Medication = medication.toDF()
    //Medication.show()
    Medication.createOrReplaceTempView("medication");
    
    val undergoes= lines3.map(undergoesmapper)
    undergoes.cache
  
    val Undergoes = undergoes.toDF()
    //Undergoes.show()
    Undergoes.createOrReplaceTempView("undergoes");

    val procedures = lines4.map(proceduresmapper)
    procedures.cache
    
    val Procedures = procedures.toDF()
    //Procedures.show()
    Procedures.createOrReplaceTempView("procedures");
    
    val hivetable1 = spark.table("healthcare.patient")
    hivetable1.createOrReplaceTempView("patient");
    //hivetable1.show()
    
    val hivetable2 = spark.table("healthcare.physician")
    hivetable2.createOrReplaceTempView("physician");    
    //hivetable2.show()  
    
    //Question 1
                                                       
    val freqencyReOrder1 = spark.sql("SELECT t.name AS Nameofthepatient,p.name AS Nameofthephysician,a.examinationroom AS RoomNo FROM patient t JOIN appointment a ON a.patient=t.ssn JOIN physician p ON a.physicianid=p.employee_id WHERE a.prepnurse IS NULL");
    freqencyReOrder1.show()
    
    //Question 2 
    val freqencyReOrder2 = spark.sql("select t.name , p.name,m.name FROM patient t JOIN prescribes s ON s.patient = t.ssn JOIN physician p ON  s.physician = p.employee_id JOIN medication m ON s.medication = m.code WHERE s.appointment IS NOT NULL");
    freqencyReOrder2.show()
    
    //Question 3
    //val freqencyReOrder3 = spark.sql("select p.name , pr.name,u.date,pt.name FROM physician p, undergoes u, patient pt, procedure pr  WHERE  u.patient= pt.SSN AND u.procedures = pr.code AND u.physician = p.employeeID AND NOT EXISTS(select * FROM trained_in t WHERE t.treatment = u.procedures AND t.physician = u.physician)");
    //freqencyReOrder3.show()
    
    //Question 4
    val freqencyReOrder4 = spark.sql("select p.name , a.examinationroom, a.start FROM patient p JOIN appointment a ON p.SSN = a.patient");  
    freqencyReOrder4.show()
    
    spark.stop() 
  }
}