import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

from mongoDBoperation import MongodbOperation
# from python file import class name
from AzureBlobStorage.AzureStorageMgmt import AzureBlobManagement
# from folder and python file name import class name
from application_logging.loggerDB import App_LoggerDB

class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None

        """

    def     __init__(self,log_database,log_collection,execution_id):
        self.log_database=log_database
        self.log_collection=log_collection
        self.execution_id=execution_id
        self.mongoDBObject = MongodbOperation()
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.az_blob_mgt=AzureBlobManagement()

    def remove_columns(self,data,columns):
        """
                Method Name: remove_columns
                Description: This method removes the given columns from a pandas dataframe.
                Output: A pandas DataFrame after removing the specified columns.
                On Failure: Raise Exception

                Written By: iNeuron Intelligence
                Version: 1.0
                Revisions: None

        """
        self.log_db_writer.log(self.log_database,self.log_collection,"Entered the remove_columns method of the Preprocessor class")
        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            print("Useful data in dataframe")
            print(self.useful_data)
            self.log_db_writer.log(self.log_database,self.log_collection,"Column removal Successful.Exited the "
                                                                         "remove_columns method of the Preprocessor class")

            return self.useful_data

        except Exception as e:
            self.log_db_writer.log(self.log_database,self.log_collection,"Exception occured in remove_columns method"
                                                                         " of the Preprocessor class. Exception message:  "+str(e))
            self.log_db_writer.log(self.log_database,self.log_collection,'Column removal Unsuccessful. Exited the '
                                                                         'remove_columns method of the Preprocessor class')

            raise Exception()

    def separate_label_feature(self, data, label_column_name):
        """
                        Method Name: separate_label_feature
                        Description: This method separates the features and a Label Coulmns.
                        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
                        On Failure: Raise Exception

                        Written By: iNeuron Intelligence    
                        Version: 1.0
                        Revisions: None

                """
        self.log_db_writer.log(self.log_database,self.log_collection,'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified ,i,e output column and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.log_db_writer.log(self.log_database,self.log_collection,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            print(self.X, self.Y)
            return self.X,self.Y
        except Exception as e:
            self.log_db_writer.log(self.log_database, self.log_collection,
                                   'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.log_db_writer.log(self.log_database, self.log_collection,'Label Separation Unsuccessful. Exited the separate_label_feature'
                                                                          ' method of the Preprocessor class')
            raise Exception()

    def is_null_present(self,data):
        """
                                Method Name: is_null_present
                                Description: This method checks whether there are null values present in the pandas Dataframe or not.
                                Output: Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
                                On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                        """
        self.log_db_writer.log(self.log_database, self.log_collection, 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        try:
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in self.null_counts:
                if i>0:
                    self.null_present=True
                    break
            if(self.null_present): # write the logs to see which columns have null values
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                print(dataframe_with_null)
                #dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
                self.az_blob_mgt.saveDataFrametoCSV("preprocessing-data", "null_values.csv",
                                                    data_frame=dataframe_with_null)
            self.log_db_writer.log(self.log_database, self.log_collection,'Finding missing values is a success.Data written'
                                                                          '[preprocessing-data] to the null values file. '
                                                                          'Exited the is_null_present method of the Preprocessor class')
            return self.null_present
        except Exception as e:
            self.log_db_writer.log(self.log_database, self.log_collection, 'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.log_db_writer.log(self.log_database, self.log_collection,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def impute_missing_values(self, data):
        """
                                        Method Name: impute_missing_values
                                        Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                                        Output: A Dataframe which has all the missing values imputed.
                                        On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                        Version: 1.0
                                        Revisions: None
                     """
        self.log_db_writer.log(self.log_database, self.log_collection, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        try:
            imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data=pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.log_db_writer.log(self.log_database, self.log_collection, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.new_data
        except Exception as e:
            self.log_db_writer.log(self.log_database, self.log_collection, 'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.log_db_writer.log(self.log_database, self.log_collection, 'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()

    def get_columns_with_zero_std_deviation(self,data):
        """
                                                Method Name: get_columns_with_zero_std_deviation
                                                Description: This method finds out the columns which have a standard deviation of zero.
                                                Output: List of the columns with standard deviation of zero
                                                On Failure: Raise Exception

                                                Written By: iNeuron Intelligence
                                                Version: 1.0
                                                Revisions: None
                             """
        self.log_db_writer.log(self.log_database, self.log_collection, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            self.log_db_writer.log(self.log_database, self.log_collection, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            print(self.col_to_drop)
            return self.col_to_drop

        except Exception as e:
            self.log_db_writer.log(self.log_database, self.log_collection, 'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            self.log_db_writer.log(self.log_database, self.log_collection, 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()