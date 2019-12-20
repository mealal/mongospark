# Using Spark [Magellan](https://github.com/harsha2010/magellan) for processing MongoDB GeoJSON data

Used data from the following article: [MongoDB Geospatial Tutorial](https://docs.mongodb.com/manual/tutorial/geospatial-tutorial/).

The following files were imported into MongoDB data database:
[neighborhoods.json](https://raw.githubusercontent.com/mongodb/docs-assets/geospatial/neighborhoods.json),
[restaurants.json](https://raw.githubusercontent.com/mongodb/docs-assets/geospatial/restaurants.json).

### Spark server/shell commands
```bash
spark-shell --packages harsha2010:magellan:1.0.4-s_2.11,org.mongodb.spark:mongo-spark-connector_2.11:2.2.6
```
```scala
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import magellan.{Point, Polygon, PolyLine}
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._

val readConfig1 = ReadConfig(Map("uri" -> "mongodb://ec2-3-91-208-52.compute-1.amazonaws.com,ec2-54-92-210-207.compute-1.amazonaws.com,ec2-34-239-102-19.compute-1.amazonaws.com/test.neighborhoods"))
val readConfig2 = ReadConfig(Map("uri" -> "mongodb://ec2-3-91-208-52.compute-1.amazonaws.com,ec2-54-92-210-207.compute-1.amazonaws.com,ec2-34-239-102-19.compute-1.amazonaws.com/test.restaurants"))

val neighborhoods = MongoSpark.load(sc, readConfig1).toDF
val restaurants = MongoSpark.load(sc, readConfig2).toDF

val toPoint = udf((d:WrappedArray[Double]) => Point(d(0),d(1)))
val toPolygon = udf{(points: Seq[Point]) => Polygon(Array(0), points.toArray)}

var restaurants_points = restaurants.select(col("name") as "restaurant", col("location.coordinates") as "point").withColumn("point", toPoint(col("point"))).cache()
val neighborhoods_polygons = neighborhoods.select(col("name"), col("geometry.coordinates").getItem(0) as "polygon").withColumn("polygon", explode_outer(col("polygon"))).select(col("name"), col("polygon").getItem(0).cast(DoubleType) as "x", col("polygon").getItem(1).cast(DoubleType) as "y").select(col("name"), array(col("x"), col("y")) as "point").withColumn("point", toPoint(col("point"))).groupBy(col("name") as "neighborhood").agg(collect_list(col("point")) as "polygon").withColumn("polygon", toPolygon(col("polygon"))).cache()

restaurants_points.join(neighborhoods_polygons).where($"point" within $"polygon").filter("restaurant != ''").select(col("restaurant"), col("neighborhood")).orderBy(asc("restaurant")).show(20, false)

```
