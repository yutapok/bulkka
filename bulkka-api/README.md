# bulkka-api

## Usage
### s3 input
```scala
$ sbt console

$ scala> import bulkka.api._
import bulkka.api._

$ scala> bc
res19: bulkka.BulkkaContext.type = bulkka.BulkkaContext$@90fc423

$ scala> val keysF = S3Gets.impl.keys("bucket", "/path/to/files")
$ scala> val retF = bc.s3.runGraphGets(None, Some(keysF))
$ retF.map(
    seq => seq.map(
        s3d => s3d.contents.toString
    ).mkString
).foreach(println)
<content of file>
```
