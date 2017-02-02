package se.kth.climate.fast

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import se.kth.climate.fast.common.Metadata
import se.kth.climate.fast.common.time.DateUnit
import org.apache.hadoop.fs.Path
import org.apache.avro.mapred.FsInput
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.file.DataFileReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import ucar.nc2.time.Calendar
import ucar.nc2.time.CalendarPeriod;
import scala.reflect.runtime.universe
import com.typesafe.scalalogging.LazyLogging

object FAST extends LazyLogging {

  def load(path: String, config: Configuration)(implicit sqlContext: SQLContext): Pair[Option[Metadata], DataFrame] = {
    val metaPath = new Path(path + ".meta");

    val metaInput = new FsInput(metaPath, config);
    val metaReader = new ReflectDatumReader[Metadata](Metadata.AVRO);
    val metaFileReader = DataFileReader.openReader(metaInput, metaReader);
    val meta = if (metaFileReader.hasNext()) {
      Some(metaFileReader.next())
    } else {
      None
    }
    metaFileReader.close();
    val df = sqlContext.read.parquet(path);
    return (meta, df)
  }

  def registerUDFs(field: String, metaO: Option[Metadata])(implicit sqlContext: SQLContext): Pair[UserDefinedFunction, UserDefinedFunction] = {
    val unit = findCalendarUnit(field, metaO);
    val year = sqlContext.udf.register("yearOf", (input: Double) => {
      unit.makeCalendarDate(input).getFieldValue(CalendarPeriod.Field.Year)
    })
    val month = sqlContext.udf.register("monthOf", (input: Double) => {
      unit.makeCalendarDate(input).getFieldValue(CalendarPeriod.Field.Month)
    })
    (year, month)
  }

  private def findCalendarUnit(field: String, metaO: Option[Metadata]): DateUnit = {
    metaO match {
      case Some(meta) => {
        val fieldVar = meta.findVariable(field);
        if (fieldVar == null) {
          logger.warn(s"No variable $field in $meta. Falling back to default DateUnit");
          return DateUnit.DEFAULT
        }
        val calAttr = fieldVar.getAttribute("calendar");
        if (calAttr == null) {
          logger.warn(s"No attribute calendar in $fieldVar. Falling back to default DateUnit");
          return DateUnit.DEFAULT
        }
        val cal = Calendar.get(calAttr);
        if (cal == null) {
          logger.warn(s"No calendar for $calAttr. Falling back to default DateUnit");
          return DateUnit.DEFAULT
        }
        val unit = new DateUnit(cal, fieldVar.getUnits);
        if (unit == null) {
          logger.warn(s"No DateUnit for $fieldVar.getUnits. Falling back to default DateUnit");
          return DateUnit.DEFAULT
        }
        return unit
      }
      case None => logger.warn("No meta information! Falling back to default DateUnit"); DateUnit.DEFAULT
    }
  }
}
