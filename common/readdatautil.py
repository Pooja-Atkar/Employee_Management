class ReadDataUtil:
    # def __init__(self):


    def readCsv(self, spark, path, schema=None, inferschema=True, header=True, sep=","):
        """
        Returns new dataframe by reading provided csv file
        :param spark: spark session
        :param path: csv file path
        :param schema: provide schema, req when inferschema is false
        :param inferschema: if true: detect file path
        :param header: if true: input csv file has header
        :param sep: default: "," specify separator present in csv file
        :return:
        """
        if (inferschema is False) and (schema == None):
            raise Exception("please provide inferschema as true else provide schema for given input file")
        if schema == None:
            readdf = spark.read.csv(path=path, inferSchema=inferschema, header=header, sep=sep)
        else:
            readdf = spark.read.csv(path=path, schema=schema, header=header, sep=sep)
        return readdf