if( !require("SparkR") ) {
  # Must be adopted for own system
  Sys.setenv(SPARK_HOME="C:/Users/sherbold/spark")
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
}
library(SparkR)

if( !require("rSHARK") ) {
  if (!require("devtools")) install.packages("devtools")
  library(devtools)
  install_github("smartshark/rSHARK")
}
library(rSHARK)

# local Spark session with 2 cores
SPARK_MASTER <- "local[2]"

# Must be adopted for own system
SPARKSHARK_JAR <- "C:/Users/sherbold/sparkSHARK/sparkSHARK-0.0.1-jar-with-dependencies.jar"

# Remove MongoDB
JAVA_OPTIONS <- paste("-Dspark.executorEnv.mongo.uri=141.5.113.177",
                      "-Dspark.executorEnv.mongo.dbname=smartshark_test")

sparkSession <- sparkR.session(master=SPARK_MASTER,
                               sparkConfig=list(spark.driver.extraClassPath=SPARKSHARK_JAR,
                                                spark.driver.extraLibraryPath=SPARKSHARK_JAR,
                                                spark.driver.extraJavaOptions=JAVA_OPTIONS),
                               sparkJars=SPARKSHARK_JAR)

mongoDBUtils <- rShark.createMongoDBUtils(sparkSession)

# Load all code entities including their abstraction levels (e.g., class, method, ...) and Java product metrics for classes
codeEntities <- rShark.loadDataLogical(mongoDBUtils,
                       "code_entity_state",
                       list(c("AbstractionLevel"), c("ProductMetric", "JavaClass")))

# filter to keep only classes
codeEntities <- filter(codeEntities, codeEntities$ce_type=="class")

# setup training and test data
selectedFeatures <- select(codeEntities, "metrics.LOC", "metrics.WMC", "metrics.CBO")
trainData <- sample(selectedFeatures, withReplacement=FALSE, fraction=0.5, seed=42)
testData <- except(selectedFeatures, trainData)

# fit a GLM model to predict the LOC from WMC and CBO
glm <- spark.glm(trainData, LOC ~ WMC + CBO, family = "gaussian")

# summarize the result
summary(glm)

# make predictions on test data
glmPredictions <- predict(glm, testData)
summary(gaussianPredictions)
