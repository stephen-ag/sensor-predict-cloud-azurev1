import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods

from mongoDBoperation import MongodbOperation
# from python file import class name
from AzureBlobStorage.AzureStorageMgmt import AzureBlobManagement
# from folder and python file name import class name
from application_logging.loggerDB import App_LoggerDB

class KMeansClustering:
    """
            This class shall  be used to divide the data into clusters before training.

            Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None

            """

    def __init__(self,execution_id):
        #self.file_object = file_object
        #self.logger_object = logger_object
        self.log_database="wafer_training_log"
        self.log_collection="training_main_log"
        self.execution_id=execution_id
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.mongoDBObject = MongodbOperation()
        self.az_blob_mgt=AzureBlobManagement()

    def elbow_plot(self,data):
        """
                        Method Name: elbow_plot
                        Description: This method saves the plot to decide the optimum number of clusters to the file.
                        Output: A picture saved to the directory
                        On Failure: Raise Exception

                        Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                """
        self.log_db_writer.log(self.log_database, self.log_collection, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss=[] # initializing an empty list
        try:
            for i in range (1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42) # initializing the KMeans object
                kmeans.fit(data) # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)# intertia_ gives the Wcss
           # plt.plot(range(1,11),wcss) # creating the graph between WCSS and the number of clusters
           # plt.title('The Elbow Method')
           # plt.xlabel('Number of clusters')
           # plt.ylabel('WCSS')
            #plt.show()
            #plt.savefig('preprocessing_data/K-Means_Elbow.PNG') # saving the elbow plot locally
            self.az_blob_mgt.saveObject("preprocessing-data", "K-Means-Elbow", plt)
            # finding the value of the optimum cluster programmatically
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.log_db_writer.log(self.log_database, self.log_collection, 'The optimum number of clusters is: '+str(self.kn.knee)+' . Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.log_db_writer.log(self.log_database, self.log_collection,'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.log_db_writer.log(self.log_database, self.log_collection,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()

    def create_clusters(self,data,number_of_clusters):
        """
                                Method Name: create_clusters
                                Description: Create a new dataframe consisting of the cluster information.
                                Output: A datframe with cluster column
                                On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                        """
        self.log_db_writer.log(self.log_database, self.log_collection, 'Entered the create_clusters method of the KMeansClustering class')
        self.data=data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            #self.data = self.data[~self.data.isin([np.nan, np.inf, -np.inf]).any(1)]
            self.y_kmeans=self.kmeans.fit_predict(data) #  divide data into clusters

            self.file_op = file_methods.File_Operation(self.log_database,self.log_collection,self.execution_id)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') # saving the KMeans model to directory
                                                                                    # passing 'Model' as the functions need three parameters

            self.data['Cluster']=self.y_kmeans  # create a new column in dataset for storing the cluster information
            self.log_db_writer.log(self.log_database, self.log_collection, 'succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data
        except Exception as e:
            self.log_db_writer.log(self.log_database, self.log_collection,'Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.log_db_writer.log(self.log_database, self.log_collection,'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()