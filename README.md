
# SparkSQL

* After dealing with `select` and `selectExpr` when selecting columns
* After dealing with `where` when selecting rows
* After dealing with `joins` when joining information



```scala
import org.apache.spark.sql.types._
val bookSchema = new StructType(Array(
   new StructField("bookID", IntegerType, false),
   new StructField("title", StringType, false),
   new StructField("authors", StringType, false),
   new StructField("average_rating", FloatType, false),
   new StructField("isbn", StringType, false),
   new StructField("isbn13", StringType, false),
   new StructField("language_code", StringType, false),
   new StructField("num_pages", IntegerType, false),
   new StructField("ratings_count", IntegerType, false),
   new StructField("text_reviews_count", IntegerType, false)))

val booksDF = spark.read.format("csv")
                         .schema(bookSchema)
                         .option("header", "true")
                         .option("encoding", "UTF-8")
                         .load("../data/books.csv")
booksDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- num_pages: integer (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- text_reviews_count: integer (nullable = true)
    
    




    import org.apache.spark.sql.types._
    bookSchema: org.apache.spark.sql.types.StructType = StructType(StructField(bookID,IntegerType,false), StructField(title,StringType,false), StructField(authors,StringType,false), StructField(average_rating,FloatType,false), StructField(isbn,StringType,false), StructField(isbn13,StringType,false), StructField(language_code,StringType,false), StructField(num_pages,IntegerType,false), StructField(ratings_count,IntegerType,false), StructField(text_reviews_count,IntegerType,false))
    booksDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



## Establishing a table name 

* In Spark we require a table name so that we can query the information
* This can be done with the following calls from the `DataFrame`
  * `createOrReplaceTempView(viewName: String)`
  * `createGlobalTempView(viewName: String)`
  * `createOrReplaceGlobalTempView(viewName: String)`
* The only difference above is the scope:
  * Global refers to the life of the Spark application
  * Non-global is per the scope while in use


```scala
booksDF.createOrReplaceTempView("books")
```

## Running a SQL Query

* SQL Queries are run using the `sql` method on the [`SparkSession`](https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.SparkSession)
* The `sql` call returns a `DataFrame`
* Use for the table name the name provided to the `createOrReplaceTempView`, `createOrReplaceGlobalTempView`, or `createGlobalTempView`


```scala
spark.sql("select * from books").show(10)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|      652|      1944099|             26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|      870|      1996446|             27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|      320|      5629932|             70390|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|      352|         6267|               272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|      435|      2149872|             33964|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|          4.78|0439682584|9780439682589|          eng|     2690|        38872|               154|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|          3.69|0976540606|9780976540601|        en-US|      152|           18|                 1|
    |    10|Harry Potter Coll...|        J.K. Rowling|          4.73|0439827604|9780439827607|          eng|     3342|        27410|               820|
    |    12|The Ultimate Hitc...|       Douglas Adams|          4.38|0517226952|9780517226957|          eng|      815|         3602|               258|
    |    13|The Ultimate Hitc...|       Douglas Adams|          4.38|0345453743|9780345453747|          eng|      815|       240189|              3954|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 10 rows
    
    

## Multiline Queries

* You can express multiline using a Scala Smart String
* You can use optionally use `stripMargin` for aesthetics


```scala
val result = spark.sql("""SELECT title, authors, num_pages
                          | FROM books 
                          | WHERE average_rating > 4.5""".stripMargin)
result.show(10)
```

    +--------------------+--------------------+---------+
    |               title|             authors|num_pages|
    +--------------------+--------------------+---------+
    |Harry Potter and ...|J.K. Rowling-Mary...|      652|
    |Harry Potter and ...|J.K. Rowling-Mary...|      435|
    |Harry Potter Boxe...|J.K. Rowling-Mary...|     2690|
    |Harry Potter Coll...|        J.K. Rowling|     3342|
    |J.R.R. Tolkien 4-...|      J.R.R. Tolkien|     1728|
    |The Lord of the R...|Chris   Smith-Chr...|      218|
    |100 Years of Lync...|      Ralph Ginzburg|      270|
    |The Gettysburg Ad...|Abraham Lincoln-M...|       32|
    |Fullmetal Alchemi...|Hiromu Arakawa-Ak...|      192|
    |Fullmetal Alchemi...|Hiromu Arakawa-Ak...|      192|
    +--------------------+--------------------+---------+
    only showing top 10 rows
    
    




    result: org.apache.spark.sql.DataFrame = [title: string, authors: string ... 1 more field]
    



## Mixing and Matching Queries

* You can choose to do part of the query using SparkSQL, and part in `DataFrame` and `DataSet` method calls


```scala
val result = spark.sql("""SELECT title, authors, num_pages
                          | FROM books""".stripMargin)
                  .where($"average_rating" > 4.5)
result.show(10)
```

    +--------------------+--------------------+---------+
    |               title|             authors|num_pages|
    +--------------------+--------------------+---------+
    |Harry Potter and ...|J.K. Rowling-Mary...|      652|
    |Harry Potter and ...|J.K. Rowling-Mary...|      435|
    |Harry Potter Boxe...|J.K. Rowling-Mary...|     2690|
    |Harry Potter Coll...|        J.K. Rowling|     3342|
    |J.R.R. Tolkien 4-...|      J.R.R. Tolkien|     1728|
    |The Lord of the R...|Chris   Smith-Chr...|      218|
    |100 Years of Lync...|      Ralph Ginzburg|      270|
    |The Gettysburg Ad...|Abraham Lincoln-M...|       32|
    |Fullmetal Alchemi...|Hiromu Arakawa-Ak...|      192|
    |Fullmetal Alchemi...|Hiromu Arakawa-Ak...|      192|
    +--------------------+--------------------+---------+
    only showing top 10 rows
    
    




    result: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [title: string, authors: string ... 1 more field]
    



## Splitting a column 

* We can try the same logic as before when we were working with a `DataFrame`


```scala
spark.sql("""SELECT (split(authors, '-')[0]) as primary_author from books""").show(10)
```

    +--------------------+
    |      primary_author|
    +--------------------+
    |        J.K. Rowling|
    |        J.K. Rowling|
    |        J.K. Rowling|
    |        J.K. Rowling|
    |        J.K. Rowling|
    |        J.K. Rowling|
    |W. Frederick Zimm...|
    |        J.K. Rowling|
    |       Douglas Adams|
    |       Douglas Adams|
    +--------------------+
    only showing top 10 rows
    
    

## Trying a `groupBy` in the same process as we did in querying example


```scala
spark.sql("""SELECT (split(authors, '-')[0]) as primary_author, average_rating, ratings_count from books""").show(10)
```

    +--------------------+--------------+-------------+
    |      primary_author|average_rating|ratings_count|
    +--------------------+--------------+-------------+
    |        J.K. Rowling|          4.56|      1944099|
    |        J.K. Rowling|          4.49|      1996446|
    |        J.K. Rowling|          4.47|      5629932|
    |        J.K. Rowling|          4.41|         6267|
    |        J.K. Rowling|          4.55|      2149872|
    |        J.K. Rowling|          4.78|        38872|
    |W. Frederick Zimm...|          3.69|           18|
    |        J.K. Rowling|          4.73|        27410|
    |       Douglas Adams|          4.38|         3602|
    |       Douglas Adams|          4.38|       240189|
    +--------------------+--------------+-------------+
    only showing top 10 rows
    
    


```scala
spark.sql("""DROP VIEW primary_author_books""")
```




    res36: org.apache.spark.sql.DataFrame = []
    




```scala
spark.sql("""CREATE TEMP VIEW
                 primary_author_books AS SELECT (split(authors, '-')[0])
                 as primary_author, average_rating, ratings_count from books""")
```




    res37: org.apache.spark.sql.DataFrame = []
    




```scala
spark.sql("SELECT * from primary_author_books").show(10)
```

    +--------------------+--------------+-------------+
    |      primary_author|average_rating|ratings_count|
    +--------------------+--------------+-------------+
    |        J.K. Rowling|          4.56|      1944099|
    |        J.K. Rowling|          4.49|      1996446|
    |        J.K. Rowling|          4.47|      5629932|
    |        J.K. Rowling|          4.41|         6267|
    |        J.K. Rowling|          4.55|      2149872|
    |        J.K. Rowling|          4.78|        38872|
    |W. Frederick Zimm...|          3.69|           18|
    |        J.K. Rowling|          4.73|        27410|
    |       Douglas Adams|          4.38|         3602|
    |       Douglas Adams|          4.38|       240189|
    +--------------------+--------------+-------------+
    only showing top 10 rows
    
    


```scala
spark.sql("""SELECT primary_author, 
                    avg(average_rating) as avg_author_rating, 
                    sum(ratings_count) as total_ratings
                    FROM primary_author_books
                    GROUP BY primary_author""").show(10)
```

    +--------------------+------------------+-------------+
    |      primary_author| avg_author_rating|total_ratings|
    +--------------------+------------------+-------------+
    |          James Frey| 3.630000114440918|       195863|
    |     Eric Klinenberg|3.8399999141693115|          674|
    |     Karen Armstrong| 3.971249997615814|        67247|
    |                Éric|               3.5|         2080|
    |          Dava Sobel|3.8925000429153442|        67718|
    |        Helena Grice| 3.700000047683716|           10|
    |         Ann Rinaldi|3.7899999618530273|         4988|
    |         Ann Beattie| 3.440000057220459|         1174|
    |Brian Michael Bendis|3.8899999856948853|         1665|
    |Michael Eliot Howard| 4.050000190734863|          190|
    +--------------------+------------------+-------------+
    only showing top 10 rows
    
    


```scala
spark.sql("""SELECT primary_author, 
                    avg(average_rating) as author_average_rating,
                    (avg(average_rating) * sum(ratings_count)) as weighted_average
                    FROM primary_author_books
                    GROUP BY primary_author""").show(10)
```

    +--------------------+---------------------+------------------+
    |      primary_author|author_average_rating|  weighted_average|
    +--------------------+---------------------+------------------+
    |          James Frey|    3.630000114440918| 710982.7124147415|
    |     Eric Klinenberg|   3.8399999141693115| 2588.159942150116|
    |     Karen Armstrong|    3.971249997615814|267054.64858967066|
    |                Éric|                  3.5|            7280.0|
    |          Dava Sobel|   3.8925000429153442| 263592.3179061413|
    |        Helena Grice|    3.700000047683716| 37.00000047683716|
    |         Ann Rinaldi|   3.7899999618530273|  18904.5198097229|
    |         Ann Beattie|    3.440000057220459| 4038.560067176819|
    |Brian Michael Bendis|   3.8899999856948853| 6476.849976181984|
    |Michael Eliot Howard|    4.050000190734863|  769.500036239624|
    +--------------------+---------------------+------------------+
    only showing top 10 rows
    
    


```scala
spark.sql("""SELECT primary_author, 
                    avg(average_rating) as author_average_rating,
                    (avg(average_rating) * sum(ratings_count)) as weighted_average
                    FROM primary_author_books
                    GROUP BY primary_author
                    ORDER BY weighted_average DESC""").show(10)
```

    +-------------------+---------------------+--------------------+
    |     primary_author|author_average_rating|    weighted_average|
    +-------------------+---------------------+--------------------+
    |       J.K. Rowling|    4.512857096535819| 6.303455899299431E7|
    |     J.R.R. Tolkien|    4.235945927130209| 2.464259001540443E7|
    |       Stephen King|    4.009765664115548|2.2012037658088364E7|
    |William Shakespeare|   3.9309734812879986|1.8514315105711687E7|
    |          Dan Brown|    3.799130460490351|1.6799435769329652E7|
    |    Stephenie Meyer|   3.5899999141693115|1.5687341094942808E7|
    |    Nicholas Sparks|    3.997692291553204|1.2736339818582058E7|
    | George R.R. Martin|    4.148571389062064|1.1157126408029623E7|
    |      J.D. Salinger|   3.9745454137975518|1.0545069139162388E7|
    |      George Orwell|    4.166111177868313|   9673339.371115392|
    +-------------------+---------------------+--------------------+
    only showing top 10 rows
    
    


```scala
spark.sql("""SELECT primary_author, 
                    avg(average_rating) as author_average_rating,
                    (avg(average_rating) * sum(ratings_count)) as weighted_average
                    FROM primary_author_books
                    GROUP BY primary_author
                    ORDER BY weighted_average DESC
                    LIMIT 10""").show(10)
```

    +-------------------+---------------------+--------------------+
    |     primary_author|author_average_rating|    weighted_average|
    +-------------------+---------------------+--------------------+
    |       J.K. Rowling|    4.512857096535819| 6.303455899299431E7|
    |     J.R.R. Tolkien|    4.235945927130209| 2.464259001540443E7|
    |       Stephen King|    4.009765664115548|2.2012037658088364E7|
    |William Shakespeare|   3.9309734812879986|1.8514315105711687E7|
    |          Dan Brown|    3.799130460490351|1.6799435769329652E7|
    |    Stephenie Meyer|   3.5899999141693115|1.5687341094942808E7|
    |    Nicholas Sparks|    3.997692291553204|1.2736339818582058E7|
    | George R.R. Martin|    4.148571389062064|1.1157126408029623E7|
    |      J.D. Salinger|   3.9745454137975518|1.0545069139162388E7|
    |      George Orwell|    4.166111177868313|   9673339.371115392|
    +-------------------+---------------------+--------------------+
    
    

### Create a large query to look impressive


```scala
spark.sql("""DROP VIEW primary_author_books""")
```




    res58: org.apache.spark.sql.DataFrame = []
    




```scala
spark.sql("""SELECT primary_author, 
                    avg(average_rating) as author_average_rating,
                    (avg(average_rating) * sum(ratings_count)) as weighted_average
                    FROM (
                      SELECT (split(authors, '-')[0])
                        as primary_author, average_rating, ratings_count from books
                    )
                    GROUP BY primary_author
                    ORDER BY weighted_average DESC
                    LIMIT 10""").show(10)
```

    +-------------------+---------------------+--------------------+
    |     primary_author|author_average_rating|    weighted_average|
    +-------------------+---------------------+--------------------+
    |       J.K. Rowling|    4.512857096535819| 6.303455899299431E7|
    |     J.R.R. Tolkien|    4.235945927130209| 2.464259001540443E7|
    |       Stephen King|    4.009765664115548|2.2012037658088364E7|
    |William Shakespeare|   3.9309734812879986|1.8514315105711687E7|
    |          Dan Brown|    3.799130460490351|1.6799435769329652E7|
    |    Stephenie Meyer|   3.5899999141693115|1.5687341094942808E7|
    |    Nicholas Sparks|    3.997692291553204|1.2736339818582058E7|
    | George R.R. Martin|    4.148571389062064|1.1157126408029623E7|
    |      J.D. Salinger|   3.9745454137975518|1.0545069139162388E7|
    |      George Orwell|    4.166111177868313|   9673339.371115392|
    +-------------------+---------------------+--------------------+
    
    

### One more nested statement


```scala
spark.sql("""SELECT primary_author, author_average_rating 
                    FROM (
                       SELECT primary_author, 
                              avg(average_rating) as author_average_rating,
                              (avg(average_rating) * sum(ratings_count)) as weighted_average
                       FROM (
                          SELECT (split(authors, '-')[0]) as primary_author, 
                          average_rating, 
                          ratings_count from books
                       )
                       GROUP BY primary_author
                       ORDER BY weighted_average DESC
                       LIMIT 10)""").show(10)
```

    +-------------------+---------------------+
    |     primary_author|author_average_rating|
    +-------------------+---------------------+
    |       J.K. Rowling|    4.512857096535819|
    |     J.R.R. Tolkien|    4.235945927130209|
    |       Stephen King|    4.009765664115548|
    |William Shakespeare|   3.9309734812879986|
    |          Dan Brown|    3.799130460490351|
    |    Stephenie Meyer|   3.5899999141693115|
    |    Nicholas Sparks|    3.997692291553204|
    | George R.R. Martin|    4.148571389062064|
    |      J.D. Salinger|   3.9745454137975518|
    |      George Orwell|    4.166111177868313|
    +-------------------+---------------------+
    
    
