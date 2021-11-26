package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART   //CHANGED

  def IsCellInBounds(x:Double, y:Double, z:Int, minX:Double, maxX:Double, minY:Double, maxY:Double, minZ:Int, maxZ:Int): Boolean =
  {
    if ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ){
      return true
    }
    return false
  }

  def GetLocation(point: Int, minVal: Int, maxVal: Int) : Int={
    if (point == minVal || point == maxVal){
      return 1
    }
    else {
      return 0
    }
  }

  def GetCountOfNeighbors(minX:Int, minY:Int, minZ:Int, maxX:Int, maxY:Int, maxZ:Int, Xin:Int, Yin:Int, Zin:Int): Int ={
    val whereInCube: Map[Int, String] = Map(0->"in", 1 -> "plane", 2-> "edge", 3-> "corner")
    val howManyNeighbors: Map[String, Int] = Map("in" -> 26, "plane" -> 17, "edge" -> 11, "corner" -> 7)
    var locVal = 0;
    locVal += GetLocation(Xin, minX, maxX)
    locVal += GetLocation(Yin, minY, maxY)
    locVal += GetLocation(Zin, minZ, maxZ)
    var whereInCubeForMap = whereInCube.get(locVal).get.toString()
    return howManyNeighbors.get(whereInCubeForMap).get.toInt
  }

  def GetGScore(x: Int, y: Int, z: Int, numcells: Int, mean:Double, sd: Double, totalNeighbors: Int, sumAllNeighborsPoints: Int): Double ={
    val num = (sumAllNeighborsPoints.toDouble - (mean*totalNeighbors.toDouble))
    val denom = sd * math.sqrt((((numcells.toDouble * totalNeighbors.toDouble) - (totalNeighbors.toDouble * totalNeighbors.toDouble)) / (numcells.toDouble-1.0).toDouble).toDouble).toDouble
    return (num/denom).toDouble
  }
}
