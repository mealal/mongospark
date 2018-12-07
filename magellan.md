# Using Spark [Magellan](https://github.com/harsha2010/magellan) for processing MongoDB GeoJSON data

Used data from the following article:[MongoDB Geospatial Tutorial](https://docs.mongodb.com/manual/tutorial/geospatial-tutorial/).

The following files were imported into MongoDB data database:
[neighborhoods.json](https://raw.githubusercontent.com/mongodb/docs-assets/geospatial/neighborhoods.json),
[restaurants.json](https://raw.githubusercontent.com/mongodb/docs-assets/geospatial/restaurants.json).

### Spark server/shell commands
```bash
spark-shell --packages harsha2010:magellan:1.0.4-s_2.11,org.mongodb.spark:mongo-spark-connector_2.11:2.3.1<br/>
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

val readConfig1 = ReadConfig(Map("uri" -> "mongodb://mongodbserver/data.neighborhoods"))
val readConfig2 = ReadConfig(Map("uri" -> "mongodb://mongodbserver/data.restaurants"))

val neighborhoods = MongoSpark.load(sc, readConfig1).toDF
val restaurants = MongoSpark.load(sc, readConfig2).toDF

val toPoint = udf((d:WrappedArray[Double]) => Point(d(0),d(1)))
val toPolygon = udf{(points: Seq[Point]) => Polygon(Array(0), points.toArray)}

var restaurants_points = restaurants.select(col("name"), col("location.coordinates") as "point").withColumn("point", toPoint(col("point"))).cache()
val neighborhoods_polygons = neighborhoods.select(explode(col("features"))).select(col("col.properties.neighborhood") as "neighborhood", col("col.geometry.coordinates").getItem(0) as "polygon").withColumn("polygon", explode_outer(col("polygon"))).withColumn("polygon", myUDf(col("polygon"))).groupBy(col("neighborhood")).agg(collect_list(col("polygon")) as "polygon").withColumn("polygon", toPolygon(col("polygon"))).cache()
neighborhoods_polygons: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [neighborhood: string, polygon: polygon]<br/>

restaurants_points.join(neighborhoods_polygons).where($"point" within $"polygon").show()
```
