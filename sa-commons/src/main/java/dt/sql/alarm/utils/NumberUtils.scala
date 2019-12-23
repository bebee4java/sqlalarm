package dt.sql.alarm.utils

object NumberUtils {


  def getIntValue( obj:Object, defaultValue:Int) = {
    if (obj == null) {
      defaultValue
    } else {
      try {
        Integer.parseInt(obj.toString)
      } catch {
        case e:Exception => defaultValue
      }
    }

  }

}
