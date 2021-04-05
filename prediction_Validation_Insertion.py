from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
#from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DbOperationMongoDB
# from folder, python file and import class name
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
#from application_logging import logger

from application_logging.loggerDB import App_LoggerDB
# from folder import python file logger#
from AzureBlobStorage.AzureStorageMgmt import AzureBlobManagement
# from folder import python file#
from mongoDBoperation import MongodbOperation


class pred_validation:
    def __init__(self,path, execution_id):
        self.raw_data = Prediction_Data_validation(path, execution_id)
        self.dataTransform = dataTransformPredict(execution_id)
        self.dBOperationMongoDB = DbOperationMongoDB(execution_id)
        #self.dBOperation = dBOperation(execution_id)
        self.log_database = "wafer_prediction_log"
        self.log_collection = "prediction_main_log"
        self.execution_id = execution_id
        #self.log_writer = logger.App_Logger()
        self.logDB_write = App_LoggerDB(execution_id=execution_id)
        self.az_blob_mgt = AzureBlobManagement()

        #self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        #self.log_writer = logger.App_Logger()

    def prediction_validation(self):

        try:
            self.logDB_write.log(self.log_database, self.log_collection,'Start of Validation on files for prediction!!')
            #extracting values from prediction schema
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()
            #getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            #validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            #validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.logDB_write.log(self.log_database, self.log_collection,"Raw Data Validation Complete!!")

            self.logDB_write.log(self.log_database, self.log_collection,("Starting Data Transforamtion!!"))
            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.logDB_write.log(self.log_database, self.log_collection,"DataTransformation Completed!!!")
            self.logDB_write.log(self.log_database, self.log_collection,"Creating Prediction_Database and tables on the basis of given schema!!!")
            #create database with given name, if present open the connection! Create table with columns given in schema
            self.logDB_write.log(self.log_database,self.log_collection, "Creating database and collection if not exist then insert record")
            #insert csv files in the table
            self.dBOperationMongoDB.insertIntoTableGoodData(column_names)
            self.logDB_write.log(self.log_database, self.log_collection,"Insertion in Table completed!!!")
            #self.logDB_write.log(self.log_database, self.log_collection,"Deleting Good Data Folder!!!")
            #Delete the good data folder after loading files in table
            #self.raw_data.deleteExistingGoodDataTrainingFolder()
            #self.logDB_write.log(self.log_database, self.log_collection,"Good_Data folder deleted!!!")
            self.logDB_write.log(self.log_database, self.log_collection,"Moving bad files to Archive and deleting Bad_Data folder!!!")
            #Move the bad files to archive folder
            print("moving bad files to archieve")
            self.raw_data.moveBadFilesToArchiveBad()
            self.logDB_write.log(self.log_database, self.log_collection,"Bad files moved to archive!! Bad folder Deleted!!")
            self.logDB_write.log(self.log_database, self.log_collection,"Validation Operation completed!!")
            self.logDB_write.log(self.log_database, self.log_collection,"Extracting csv file from table")
            #export data in table to csvfile
            self.dBOperationMongoDB.selectingDatafromtableintocsv()

        except Exception as e:
            raise e









