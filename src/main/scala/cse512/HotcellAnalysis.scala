package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART   //CHANGED

  pickupInfo.createOrReplaceTempView("tempPickupInfo")

  spark.udf.register("IsCellInBounds", (x: Double, y:Double, z:Int) =>  HotcellUtils.IsCellInBounds(x, y, z, minX, maxX, minY, maxY, minZ, maxZ))

  val withinBoundaryPoints = spark.sql("select x,y,z from tempPickupInfo where IsCellInBounds(x, y, z) order by z,y,x").persist()
  withinBoundaryPoints.createOrReplaceTempView("withinBoundaryPointsView")

  val withinBoundaryPointsCount = spark.sql("select x,y,z,count(*) as numPoints from withinBoundaryPointsView group by z,y,x order by z,y,x").persist()
  withinBoundaryPointsCount.createOrReplaceTempView("withinBoundaryPointsCountView")

  spark.udf.register("square", (inputX: Int) => (inputX*inputX).toDouble)
  val sumofPoints = spark.sql("select count(*), sum(numPoints), sum(square(numPoints)) as squaredSumOfAllPointsInGivenArea from withinBoundaryPointsCountView")

  val sumPoints = sumofPoints.first().getLong(1) //sum x
  val sumPointsSquared = sumofPoints.first().getDouble(2) //sum (x^2)

  val meanXinFormula = sumPoints / numCells
  val StandardDeviation = math.sqrt((sumPointsSquared / numCells) - (meanXinFormula * meanXinFormula) )

  spark.udf.register("GetCountOfNeighbors", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, Xin: Int, Yin: Int, Zin: Int)
  => ((HotcellUtils.GetCountOfNeighbors(minX, minY, minZ, maxX, maxY, maxZ, Xin, Yin, Zin))))
  val NeighborVal = spark.sql("select viewWithinBoundCount1.x as x, viewWithinBoundCount1.y as y, viewWithinBoundCount1.z as z, GetCountOfNeighbors("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "viewWithinBoundCount1.x,viewWithinBoundCount1.y,viewWithinBoundCount1.z) as totalNeighborVal, count(*) as neighborValWithValidPoints, sum(viewWithinBoundCount2.numPoints) as sumAllNeighborValPoints from withinBoundaryPointsCountView as viewWithinBoundCount1, withinBoundaryPointsCountView as viewWithinBoundCount2 where (viewWithinBoundCount2.x = viewWithinBoundCount1.x+1 or viewWithinBoundCount2.x = viewWithinBoundCount1.x or viewWithinBoundCount2.x = viewWithinBoundCount1.x-1) and (viewWithinBoundCount2.y = viewWithinBoundCount1.y+1 or viewWithinBoundCount2.y = viewWithinBoundCount1.y or viewWithinBoundCount2.y = viewWithinBoundCount1.y-1) and (viewWithinBoundCount2.z = viewWithinBoundCount1.z+1 or viewWithinBoundCount2.z = viewWithinBoundCount1.z or viewWithinBoundCount2.z = viewWithinBoundCount1.z-1) group by viewWithinBoundCount1.z, viewWithinBoundCount1.y, viewWithinBoundCount1.x order by viewWithinBoundCount1.z, viewWithinBoundCount1.y, viewWithinBoundCount1.x").persist()
  NeighborVal.createOrReplaceTempView("NeighborValView")

  spark.udf.register("GetGScore", (x: Int, y: Int, z: Int, numcells: Int, mean:Double, sd: Double, totalNeighborVal: Int, sumAllNeighborValPoints: Int) => ((
    HotcellUtils.GetGScore(x, y, z, numcells, mean, sd, totalNeighborVal, sumAllNeighborValPoints))))
  val NeighborValDecreasing = spark.sql("select x, y, z, " +
    "GetGScore(x, y, z," +numCells+ ", " + meanXinFormula + ", " + StandardDeviation + ", totalNeighborVal, sumAllNeighborValPoints) as gi_statistic " +
    "from NeighborValView " +
    "order by gi_statistic desc")
  NeighborValDecreasing.createOrReplaceTempView("NeighborValDecreasingView")
  NeighborValDecreasing.show()

  val result = spark.sql("select x,y,z from NeighborValDecreasingView")

  return result
}
}
