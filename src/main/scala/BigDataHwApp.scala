
import Mixins.{DataFrameMixins, SparkSessionMixins}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object BigDataHwApp {

  def main(args: Array[String]): Unit = {

    val spark = createSession(args.headOption, Constants.appName)

    avgSalaries(spark).writeToCsv(Constants.outputDir.format("AverageSalaries"))

    hallOfFameAllStarPitchers(spark).writeToCsv(Constants.outputDir.format("HallOfFameAllStarPitchers"))

    rankings(spark).writeToCsv(Constants.outputDir.format("Rankings"))
  }

  private def avgSalaries(spark: SparkSession): sql.DataFrame = {
    val pitchingCol = col("p")
    val fieldingCol = col("f")
    val pitchersDf = spark.jdbc("Pitching", Constants.sourceConfig).select(Columns.playerId, Columns.yearId)
      .withColumn(pitchingCol.toString(), lit(true))
      .alias("pitchers")
    val fieldersDf = spark.jdbc("Fielding", Constants.sourceConfig).select(Columns.playerId, Columns.yearId)
      .withColumn(fieldingCol.toString(), lit(true))
      .alias("fielders")
    val salaries = spark.jdbc("Salaries", Constants.sourceConfig).select(Columns.playerId, Columns.salary)

    val pitchersYear = col(s"pitchers.${Fields.yearId}")
    val fieldersYear = col(s"fielders.${Fields.yearId}")
    val pitchersAndFieldersSalariesDf = pitchersDf.join(fieldersDf, Seq(Fields.playerId), "full")
      .withColumn(Aliases.year, coalesce(pitchersYear, fieldersYear))
      .drop(pitchersYear)
      .drop(fieldersYear)
      .join(salaries, usingColumn = Fields.playerId)

    pitchersAndFieldersSalariesDf.groupBy(Aliases.year)
      .agg(
        avg(when(fieldingCol === true, Columns.salary)).as(Aliases.avgSalaryFielders),
        avg(when(pitchingCol === true, Columns.salary)).as(Aliases.avgSalaryPitchers))
  }

  private def hallOfFameAllStarPitchers(spark: SparkSession): sql.DataFrame = {
    val allStarDf = spark.jdbc("AllstarFull", Constants.sourceConfig).select(Columns.playerId, Columns.yearId)
    val hallOfFameDf = spark.jdbc("HallOfFame", Constants.sourceConfig)
      .select(Columns.playerId, Columns.yearId, Columns.inducted)
    val pitchersDf = spark.jdbc("Pitching", Constants.sourceConfig).select(Columns.playerId, Columns.era)

    val allStarHallOfFameDf = allStarDf.join(hallOfFameDf, usingColumns = Seq(Fields.playerId, Fields.yearId))
    val allStarHallOfFamePitchersDf = pitchersDf.join(allStarHallOfFameDf, usingColumn = Fields.playerId)

    allStarHallOfFamePitchersDf.groupBy(Columns.playerId)
      .agg(
        avg(Columns.era).as(Aliases.era),
        count(Columns.yearId).as(Aliases.numberOfAllStarAppearances),
        min(Columns.inducted === 'Y').or(null).as(Aliases.hallOfFameInductionYear))
      .withColumnRenamed(Fields.playerId, Aliases.player)
  }

  private def rankings(spark: SparkSession): sql.DataFrame = {
    val teamsDf = spark.jdbc("Teams", Constants.sourceConfig)
      .select(Columns.teamId, Columns.yearId, Columns.rank, Columns.atBat)

    val windowGroupByYearOrdered = Window.partitionBy(Columns.yearId).orderBy(Columns.rank)
    val windowGroupByYear = Window.partitionBy(Columns.yearId)
    teamsDf.withColumn("rankNum", row_number.over(windowGroupByYearOrdered))
      .withColumn("lowestRank", last("rankNum").over(windowGroupByYear))
      .where(col("rankNum") === 1 || col("rankNum") === col("lowestRank"))
      .drop("rankNum", "lowestRank")
      .withColumnRenamed(Fields.teamId, Aliases.teamId)
      .withColumnRenamed(Fields.yearId, Aliases.year)
      .withColumnRenamed(Fields.rank, Aliases.rank)
      .withColumnRenamed(Fields.atBat, Aliases.atBat)
  }

  private def createSession(master: Option[String], appName: String) = {
    val builder = SparkSession.builder()
      .appName(appName)
    master.map(builder.master).getOrElse(builder.master("local[*]")).getOrCreate()
  }
}

object Constants {
  val appName = "big-data-engineer-hw-build"
  val outputDir = "output/%1$s"
  val sourceConfig: JdbcConfig = JdbcConfig("jdbc:mysql://localhost:3306/lahman2016", "root", "password")
}

object Fields {
  val playerId = "playerID"
  val salary = "salary"
  val era = "ERA"
  val yearId = "yearid"
  val inducted = "inducted"
  val atBat = "AB"
  val teamId = "teamId"
  val rank = "rank"
}

object Columns {
  val playerId: Column = col(Fields.playerId)
  val salary: Column = col(Fields.salary)
  val era: Column = col(Fields.era)
  val yearId: Column = col(Fields.yearId)
  val inducted: Column = col(Fields.inducted)
  val atBat: Column = col(Fields.atBat)
  val teamId: Column = col(Fields.teamId)
  val rank: Column = col(Fields.rank)
}

object Aliases {
  val player = "Player"
  val era = "ERA"
  val numberOfAllStarAppearances = "# All Star Appearances"
  val hallOfFameInductionYear = "Hall of Fame Induction Year"
  val avgSalaryPitchers = "Pitching"
  val avgSalaryFielders = "Fielding"
  val year = "Year"
  val atBat = "At Bat"
  val rank = "Rank"
  val teamId = "Team ID"
}

object Mixins {
  implicit class SparkSessionMixins(spark: SparkSession) {
    def jdbc(table: String, config: JdbcConfig): sql.DataFrame =
      spark.read
        .format("jdbc")
        .option("url", config.url)
        .option("user", config.user)
        .option("password", config.pwd)
        .option("dbtable", table)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
  }

  implicit class DataFrameMixins(df: sql.DataFrame) {
    def writeToCsv(dir: String): Unit = {
      df.write
        .mode(SaveMode.Overwrite)
        .option("header", value = true)
        .csv(dir)
    }
  }
}

case class JdbcConfig(url: String, user: String, pwd: String)