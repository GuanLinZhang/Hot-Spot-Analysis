package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART   //CHANGED
    if (pointString.isEmpty()) {
      return false
    }
    else if (pointString == null) {
      return false
    }
    else if (queryRectangle.isEmpty()) {
      return false
    }
    else if (queryRectangle == null) {
      return false
    }
    val ptStrSplit = pointString.split(",")
    val ptX = ptStrSplit(0).toDouble
    val ptY = ptStrSplit(1).toDouble
    val qRectSplit = queryRectangle.split(",")
    val upLX = qRectSplit(0).toDouble
    val upLY = qRectSplit(1).toDouble
    val btmRX = qRectSplit(2).toDouble
    val btmRY = qRectSplit(3).toDouble

    val minX = math.min(upLX,btmRX)
    val maxX = math.max(upLX,btmRX)
    val minY = math.min(upLY,btmRY)
    val maxY = math.max(upLY,btmRY)

    // Checking if point lies within rectangle
    if(ptX>=minX && ptX<=maxX && ptY >= minY && ptY <= maxY)  return true
    return false
    //return true // YOU NEED TO CHANGE THIS PART //CHANGED
  }

  // YOU NEED TO CHANGE THIS PART //NOT SURE IF ANYTHING IS NEEDED HERE

}
