# vigil_assignment

Sample Command to run in local:

/Users/macintoshimac/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --class "com.vigil.app.VigilAssignmentApp" --master local[4] target/scala-2.11/vigil_assignment-assembly-0.1.jar inputPath="s3://vigil-input-data/data/" outputPath="s3://vigil-output-data/data/" awsProfileName="default"
