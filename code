from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import ArrayType, StructType
from IPython.display import display

# function to flatten json
def flattenDataFrame(explodeDF):
    DFSchema = explodeDF.schema
    fields = DFSchema.fields
    fieldNames = DFSchema.fieldNames()
    fieldLength = len(fieldNames)

    for i in range(fieldLength):
        field = fields[i]
        fieldName = field.name
        fieldDataType = field.dataType

        if isinstance(fieldDataType, ArrayType):
            fieldNameExcludingArray = list(filter(lambda colName: colName != fieldName, fieldNames))
            fieldNamesAndExplode = fieldNameExcludingArray + [
                expr("posexplode_outer({0}) as ({1}, {2})".format(fieldName, fieldName + "_pos", fieldName))]
            arrayDF = explodeDF.select(*fieldNamesAndExplode)
            return flattenDataFrame(arrayDF)
        elif isinstance(fieldDataType, StructType):
            childFieldnames = fieldDataType.names
            structFieldNames = list(map(lambda childname: fieldName + "." + childname, childFieldnames))
            newFieldNames = list(filter(lambda colName: colName != fieldName, fieldNames)) + structFieldNames
            renamedCols = list(map(lambda x: col(x).alias(x.replace(".", "_")), newFieldNames))
            structDF = explodeDF.select(*renamedCols)
            return flattenDataFrame(structDF)
    return explodeDF
    
#creating spark session
spark = SparkSession.builder \
        .appName("Spark Join Demo") \
        .master("local[3]") \
        .getOrCreate()

# reading json file
complex_json_df = spark.read.option("multiline", "true").json("D:\\datasetss\\json_data\\complex_json_trip.json")
res = flattenDataFrame(complex_json_df)
res.printSchema()
display(res)
