-- MySQL dump 10.13  Distrib 8.0.19, for Win64 (x86_64)
--
-- Host: localhost    Database: apollo10
-- ------------------------------------------------------
-- Server version	8.0.19

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `affiliated_with`
--

DROP TABLE IF EXISTS `affiliated_with`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `affiliated_with` (
  `Physician` int NOT NULL,
  `Department` int NOT NULL,
  `PrimaryAffiliation` tinyint(1) NOT NULL,
  PRIMARY KEY (`Physician`,`Department`),
  KEY `fk_Affiliated_With_Department_DepartmentID` (`Department`),
  CONSTRAINT `fk_Affiliated_With_Department_DepartmentID` FOREIGN KEY (`Department`) REFERENCES `department` (`DepartmentID`),
  CONSTRAINT `fk_Affiliated_With_Physician_EmployeeID` FOREIGN KEY (`Physician`) REFERENCES `physician` (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `affiliated_with`
--

LOCK TABLES `affiliated_with` WRITE;
/*!40000 ALTER TABLE `affiliated_with` DISABLE KEYS */;
INSERT INTO `affiliated_with` VALUES (1,1,1),(2,1,1),(3,1,0),(3,2,1),(4,1,1),(5,1,1),(6,2,1),(7,1,0),(7,2,1),(8,1,1),(9,3,1);
/*!40000 ALTER TABLE `affiliated_with` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `appointment`
--

DROP TABLE IF EXISTS `appointment`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `appointment` (
  `AppointmentID` int NOT NULL,
  `Patient` int NOT NULL,
  `PrepNurse` int DEFAULT NULL,
  `Physician` int NOT NULL,
  `Start` datetime NOT NULL,
  `End` datetime NOT NULL,
  `ExaminationRoom` text NOT NULL,
  PRIMARY KEY (`AppointmentID`),
  KEY `fk_Appointment_Patient_SSN` (`Patient`),
  KEY `fk_Appointment_Nurse_EmployeeID` (`PrepNurse`),
  KEY `fk_Appointment_Physician_EmployeeID` (`Physician`),
  CONSTRAINT `fk_Appointment_Nurse_EmployeeID` FOREIGN KEY (`PrepNurse`) REFERENCES `nurse` (`EmployeeID`),
  CONSTRAINT `fk_Appointment_Patient_SSN` FOREIGN KEY (`Patient`) REFERENCES `patient` (`SSN`),
  CONSTRAINT `fk_Appointment_Physician_EmployeeID` FOREIGN KEY (`Physician`) REFERENCES `physician` (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `appointment`
--

LOCK TABLES `appointment` WRITE;
/*!40000 ALTER TABLE `appointment` DISABLE KEYS */;
INSERT INTO `appointment` VALUES (13216584,100000001,101,1,'2008-04-24 10:00:00','2008-04-24 11:00:00','A'),(26548913,100000002,101,2,'2008-04-24 10:00:00','2008-04-24 11:00:00','B'),(36549879,100000001,102,1,'2008-04-25 10:00:00','2008-04-25 11:00:00','A'),(46846589,100000004,103,4,'2008-04-25 10:00:00','2008-04-25 11:00:00','B'),(59871321,100000004,NULL,4,'2008-04-26 10:00:00','2008-04-26 11:00:00','C'),(69879231,100000003,103,2,'2008-04-26 11:00:00','2008-04-26 12:00:00','C'),(76983231,100000001,NULL,3,'2008-04-26 12:00:00','2008-04-26 13:00:00','C'),(86213939,100000004,102,9,'2008-04-27 10:00:00','2008-04-21 11:00:00','A'),(93216548,100000002,101,2,'2008-04-27 10:00:00','2008-04-27 11:00:00','B');
/*!40000 ALTER TABLE `appointment` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `block`
--

DROP TABLE IF EXISTS `block`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `block` (
  `BlockFloor` int NOT NULL,
  `BlockCode` int NOT NULL,
  PRIMARY KEY (`BlockFloor`,`BlockCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `block`
--

LOCK TABLES `block` WRITE;
/*!40000 ALTER TABLE `block` DISABLE KEYS */;
INSERT INTO `block` VALUES (1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3),(4,1),(4,2),(4,3);
/*!40000 ALTER TABLE `block` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `department`
--

DROP TABLE IF EXISTS `department`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `department` (
  `DepartmentID` int NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Head` int NOT NULL,
  PRIMARY KEY (`DepartmentID`),
  KEY `fk_Department_Physician_EmployeeID` (`Head`),
  CONSTRAINT `fk_Department_Physician_EmployeeID` FOREIGN KEY (`Head`) REFERENCES `physician` (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `department`
--

LOCK TABLES `department` WRITE;
/*!40000 ALTER TABLE `department` DISABLE KEYS */;
INSERT INTO `department` VALUES (1,'General Medicine',4),(2,'Surgery',7),(3,'Psychiatry',9);
/*!40000 ALTER TABLE `department` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `hospital`
--

DROP TABLE IF EXISTS `hospital`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `hospital` (
  `SNO` int DEFAULT NULL,
  `JSON` varchar(50) DEFAULT NULL,
  `Type` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hospital`
--

LOCK TABLES `hospital` WRITE;
/*!40000 ALTER TABLE `hospital` DISABLE KEYS */;
INSERT INTO `hospital` VALUES (1,'{\"H_id\":\"1\",\"Hcity\":\"bhopal\",\"Haddress\":\"xyz\"}',1),(2,'{\"H_id\":\"2\",\"Hcity\":\"indore\",\"Haddress\":\"abc\"}',1),(3,'{\"H_id\":\"3\",\"Hcity\":\"banglore\",\"Haddress\":\"pqr\"}',1);
/*!40000 ALTER TABLE `hospital` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `medication`
--

DROP TABLE IF EXISTS `medication`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `medication` (
  `Code` int NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Brand` varchar(30) NOT NULL,
  `Description` varchar(30) NOT NULL,
  PRIMARY KEY (`Code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `medication`
--

LOCK TABLES `medication` WRITE;
/*!40000 ALTER TABLE `medication` DISABLE KEYS */;
INSERT INTO `medication` VALUES (1,'Procrastin-X','X','N/A'),(2,'Thesisin','Foo Labs','N/A'),(3,'Awakin','Bar Laboratories','N/A'),(4,'Crescavitin','Baz Industries','N/A'),(5,'Melioraurin','Snafu Pharmaceuticals','N/A');
/*!40000 ALTER TABLE `medication` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `nurse`
--

DROP TABLE IF EXISTS `nurse`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `nurse` (
  `EmployeeID` int NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Position` varchar(30) NOT NULL,
  `Registered` tinyint(1) NOT NULL,
  `SSN` int NOT NULL,
  PRIMARY KEY (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `nurse`
--

LOCK TABLES `nurse` WRITE;
/*!40000 ALTER TABLE `nurse` DISABLE KEYS */;
INSERT INTO `nurse` VALUES (101,'Carla Espinosa','Head Nurse',1,111111110),(102,'Laverne Roberts','Nurse',1,222222220),(103,'Paul Flowers','Nurse',0,333333330);
/*!40000 ALTER TABLE `nurse` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `on_call`
--

DROP TABLE IF EXISTS `on_call`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `on_call` (
  `Nurse` int NOT NULL,
  `BlockFloor` int NOT NULL,
  `BlockCode` int NOT NULL,
  `OnCallStart` datetime NOT NULL,
  `OnCallEnd` datetime NOT NULL,
  PRIMARY KEY (`Nurse`,`BlockFloor`,`BlockCode`,`OnCallStart`,`OnCallEnd`),
  KEY `fk_OnCall_Block_Floor` (`BlockFloor`,`BlockCode`),
  CONSTRAINT `fk_OnCall_Block_Floor` FOREIGN KEY (`BlockFloor`, `BlockCode`) REFERENCES `block` (`BlockFloor`, `BlockCode`),
  CONSTRAINT `fk_OnCall_Nurse_EmployeeID` FOREIGN KEY (`Nurse`) REFERENCES `nurse` (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `on_call`
--

LOCK TABLES `on_call` WRITE;
/*!40000 ALTER TABLE `on_call` DISABLE KEYS */;
INSERT INTO `on_call` VALUES (101,1,1,'2008-11-04 11:00:00','2008-11-04 19:00:00'),(103,1,1,'2008-11-04 19:00:00','2008-11-05 03:00:00'),(101,1,2,'2008-11-04 11:00:00','2008-11-04 19:00:00'),(103,1,2,'2008-11-04 19:00:00','2008-11-05 03:00:00'),(102,1,3,'2008-11-04 11:00:00','2008-11-04 19:00:00'),(103,1,3,'2008-11-04 19:00:00','2008-11-05 03:00:00');
/*!40000 ALTER TABLE `on_call` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `patient`
--

DROP TABLE IF EXISTS `patient`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `patient` (
  `SSN` int NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Address` varchar(30) NOT NULL,
  `Phone` varchar(30) NOT NULL,
  `InsuranceID` int NOT NULL,
  `PCP` int NOT NULL,
  PRIMARY KEY (`SSN`),
  KEY `fk_Patient_Physician_EmployeeID` (`PCP`),
  CONSTRAINT `fk_Patient_Physician_EmployeeID` FOREIGN KEY (`PCP`) REFERENCES `physician` (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `patient`
--

LOCK TABLES `patient` WRITE;
/*!40000 ALTER TABLE `patient` DISABLE KEYS */;
INSERT INTO `patient` VALUES (100000001,'John Smith','42 Foobar Lane','555-0256',68476213,1),(100000002,'Grace Ritchie','37 Snafu Drive','555-0512',36546321,2),(100000003,'Random J. Patient','101 Omgbbq Street','555-1204',65465421,2),(100000004,'Dennis Doe','1100 Foobaz Avenue','555-2048',68421879,3);
/*!40000 ALTER TABLE `patient` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `physician`
--

DROP TABLE IF EXISTS `physician`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `physician` (
  `EmployeeID` int NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Position` varchar(30) NOT NULL,
  `SSN` int NOT NULL,
  PRIMARY KEY (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `physician`
--

LOCK TABLES `physician` WRITE;
/*!40000 ALTER TABLE `physician` DISABLE KEYS */;
INSERT INTO `physician` VALUES (1,'John Dorian','Staff Internist',111111111),(2,'Elliot Reid','Attending Physician',222222222),(3,'Christopher Turk','Surgical Attending Physician',333333333),(4,'Percival Cox','Senior Attending Physician',444444444),(5,'Bob Kelso','Head Chief of Medicine',555555555),(6,'Todd Quinlan','Surgical Attending Physician',666666666),(7,'John Wen','Surgical Attending Physician',777777777),(8,'Keith Dudemeister','MD Resident',888888888),(9,'Molly Clock','Attending Psychiatrist',999999999);
/*!40000 ALTER TABLE `physician` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `prescribes`
--

DROP TABLE IF EXISTS `prescribes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `prescribes` (
  `Physician` int NOT NULL,
  `Patient` int NOT NULL,
  `Medication` int NOT NULL,
  `Date` datetime NOT NULL,
  `Appointment` int DEFAULT NULL,
  `Dose` varchar(30) NOT NULL,
  PRIMARY KEY (`Physician`,`Patient`,`Medication`,`Date`),
  KEY `fk_Prescribes_Patient_SSN` (`Patient`),
  KEY `fk_Prescribes_Medication_Code` (`Medication`),
  KEY `fk_Prescribes_Appointment_AppointmentID` (`Appointment`),
  CONSTRAINT `fk_Prescribes_Appointment_AppointmentID` FOREIGN KEY (`Appointment`) REFERENCES `appointment` (`AppointmentID`),
  CONSTRAINT `fk_Prescribes_Medication_Code` FOREIGN KEY (`Medication`) REFERENCES `medication` (`Code`),
  CONSTRAINT `fk_Prescribes_Patient_SSN` FOREIGN KEY (`Patient`) REFERENCES `patient` (`SSN`),
  CONSTRAINT `fk_Prescribes_Physician_EmployeeID` FOREIGN KEY (`Physician`) REFERENCES `physician` (`EmployeeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `prescribes`
--

LOCK TABLES `prescribes` WRITE;
/*!40000 ALTER TABLE `prescribes` DISABLE KEYS */;
INSERT INTO `prescribes` VALUES (1,100000001,1,'2008-04-24 10:47:00',13216584,'5'),(9,100000004,2,'2008-04-27 10:53:00',86213939,'10'),(9,100000004,2,'2008-04-30 16:53:00',NULL,'5');
/*!40000 ALTER TABLE `prescribes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `procedures`
--

DROP TABLE IF EXISTS `procedures`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `procedures` (
  `Code` int NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Cost` double NOT NULL,
  PRIMARY KEY (`Code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `procedures`
--

LOCK TABLES `procedures` WRITE;
/*!40000 ALTER TABLE `procedures` DISABLE KEYS */;
INSERT INTO `procedures` VALUES (1,'Reverse Rhinopodoplasty',1500),(2,'Obtuse Pyloric Recombobulation',3750),(3,'Folded Demiophtalmectomy',4500),(4,'Complete Walletectomy',10000),(5,'Obfuscated Dermogastrotomy',4899),(6,'Reversible Pancreomyoplasty',5600),(7,'Follicular Demiectomy',25);
/*!40000 ALTER TABLE `procedures` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `room`
--

DROP TABLE IF EXISTS `room`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `room` (
  `RoomNumber` int NOT NULL,
  `RoomType` varchar(30) NOT NULL,
  `BlockFloor` int NOT NULL,
  `BlockCode` int NOT NULL,
  `Unavailable` tinyint(1) NOT NULL,
  PRIMARY KEY (`RoomNumber`),
  KEY `fk_Room_Block_PK` (`BlockFloor`,`BlockCode`),
  CONSTRAINT `fk_Room_Block_PK` FOREIGN KEY (`BlockFloor`, `BlockCode`) REFERENCES `block` (`BlockFloor`, `BlockCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `room`
--

LOCK TABLES `room` WRITE;
/*!40000 ALTER TABLE `room` DISABLE KEYS */;
INSERT INTO `room` VALUES (101,'Single',1,1,0),(102,'Single',1,1,0),(103,'Single',1,1,0),(111,'Single',1,2,0),(112,'Single',1,2,1),(113,'Single',1,2,0),(121,'Single',1,3,0),(122,'Single',1,3,0),(123,'Single',1,3,0),(201,'Single',2,1,1),(202,'Single',2,1,0),(203,'Single',2,1,0),(211,'Single',2,2,0),(212,'Single',2,2,0),(213,'Single',2,2,1),(221,'Single',2,3,0),(222,'Single',2,3,0),(223,'Single',2,3,0),(301,'Single',3,1,0),(302,'Single',3,1,1),(303,'Single',3,1,0),(311,'Single',3,2,0),(312,'Single',3,2,0),(313,'Single',3,2,0),(321,'Single',3,3,1),(322,'Single',3,3,0),(323,'Single',3,3,0),(401,'Single',4,1,0),(402,'Single',4,1,1),(403,'Single',4,1,0),(411,'Single',4,2,0),(412,'Single',4,2,0),(413,'Single',4,2,0),(421,'Single',4,3,1),(422,'Single',4,3,0),(423,'Single',4,3,0);
/*!40000 ALTER TABLE `room` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `stay`
--

DROP TABLE IF EXISTS `stay`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `stay` (
  `StayID` int NOT NULL,
  `Patient` int NOT NULL,
  `Room` int NOT NULL,
  `StayStart` datetime NOT NULL,
  `StayEnd` datetime NOT NULL,
  PRIMARY KEY (`StayID`),
  KEY `fk_Stay_Patient_SSN` (`Patient`),
  KEY `fk_Stay_Room_Number` (`Room`),
  CONSTRAINT `fk_Stay_Patient_SSN` FOREIGN KEY (`Patient`) REFERENCES `patient` (`SSN`),
  CONSTRAINT `fk_Stay_Room_Number` FOREIGN KEY (`Room`) REFERENCES `room` (`RoomNumber`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `stay`
--

LOCK TABLES `stay` WRITE;
/*!40000 ALTER TABLE `stay` DISABLE KEYS */;
INSERT INTO `stay` VALUES (3215,100000001,111,'2008-05-01 00:00:00','2008-05-04 00:00:00'),(3216,100000003,123,'2008-05-03 00:00:00','2008-05-14 00:00:00'),(3217,100000004,112,'2008-05-02 00:00:00','2008-05-03 00:00:00');
/*!40000 ALTER TABLE `stay` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `trained_in`
--

DROP TABLE IF EXISTS `trained_in`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `trained_in` (
  `Physician` int NOT NULL,
  `Treatment` int NOT NULL,
  `CertificationDate` datetime NOT NULL,
  `CertificationExpires` datetime NOT NULL,
  PRIMARY KEY (`Physician`,`Treatment`),
  KEY `fk_Trained_In_Procedures_Code` (`Treatment`),
  CONSTRAINT `fk_Trained_In_Physician_EmployeeID` FOREIGN KEY (`Physician`) REFERENCES `physician` (`EmployeeID`),
  CONSTRAINT `fk_Trained_In_Procedures_Code` FOREIGN KEY (`Treatment`) REFERENCES `procedures` (`Code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `trained_in`
--

LOCK TABLES `trained_in` WRITE;
/*!40000 ALTER TABLE `trained_in` DISABLE KEYS */;
INSERT INTO `trained_in` VALUES (3,1,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(3,2,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(3,5,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(3,6,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(3,7,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(6,2,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(6,5,'2007-01-01 00:00:00','2007-12-31 00:00:00'),(6,6,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,1,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,2,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,3,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,4,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,5,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,6,'2008-01-01 00:00:00','2008-12-31 00:00:00'),(7,7,'2008-01-01 00:00:00','2008-12-31 00:00:00');
/*!40000 ALTER TABLE `trained_in` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `undergoes`
--

DROP TABLE IF EXISTS `undergoes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `undergoes` (
  `Patient` int NOT NULL,
  `Procedures` int NOT NULL,
  `Stay` int NOT NULL,
  `DateUndergoes` datetime NOT NULL,
  `Physician` int NOT NULL,
  `AssistingNurse` int DEFAULT NULL,
  PRIMARY KEY (`Patient`,`Procedures`,`Stay`,`DateUndergoes`),
  KEY `fk_Undergoes_Procedures_Code` (`Procedures`),
  KEY `fk_Undergoes_Stay_StayID` (`Stay`),
  KEY `fk_Undergoes_Physician_EmployeeID` (`Physician`),
  KEY `fk_Undergoes_Nurse_EmployeeID` (`AssistingNurse`),
  CONSTRAINT `fk_Undergoes_Nurse_EmployeeID` FOREIGN KEY (`AssistingNurse`) REFERENCES `nurse` (`EmployeeID`),
  CONSTRAINT `fk_Undergoes_Patient_SSN` FOREIGN KEY (`Patient`) REFERENCES `patient` (`SSN`),
  CONSTRAINT `fk_Undergoes_Physician_EmployeeID` FOREIGN KEY (`Physician`) REFERENCES `physician` (`EmployeeID`),
  CONSTRAINT `fk_Undergoes_Procedures_Code` FOREIGN KEY (`Procedures`) REFERENCES `procedures` (`Code`),
  CONSTRAINT `fk_Undergoes_Stay_StayID` FOREIGN KEY (`Stay`) REFERENCES `stay` (`StayID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `undergoes`
--

LOCK TABLES `undergoes` WRITE;
/*!40000 ALTER TABLE `undergoes` DISABLE KEYS */;
INSERT INTO `undergoes` VALUES (100000001,2,3215,'2020-04-07 00:00:00',7,101),(100000001,6,3215,'2020-04-08 00:00:00',3,101),(100000001,7,3217,'2010-05-10 00:00:00',7,101),(100000004,1,3217,'2015-05-07 00:00:00',3,102),(100000004,4,3217,'2010-05-13 00:00:00',3,103),(100000004,5,3217,'2015-05-09 00:00:00',6,NULL);
/*!40000 ALTER TABLE `undergoes` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-04-07 18:46:14
