from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
import flask_monitoringdashboard as dashboard

from predictFromModel import prediction
from AzureBlobStorage.AzureStorageMgmt import AzureBlobManagement
import json
from flask import Response
import re
import uuid


def predictionTest(path=None):
    try:
        az_blb_mgt=AzureBlobManagement()
        execution_id = str(uuid.uuid4())
        if path is None:
            path = 'prediction-batch-files'
        else:
            path=path
        pred_val = pred_validation(path, execution_id)  # object initialization

        pred_val.prediction_validation()  # calling the prediction_validation function

        pred = prediction(path, execution_id)  # object initialization

        # predicting for dataset present in database
        path, json_predictions = pred.predictionFromModel()
        prediction_location="prediction-output-file"
        file_list="prediction-output-file"
        #selecting all failed file name
        return Response("Prediction File created at !!!"  +str(path) +'and few of the predictions are '+str(json.loads(json_predictions) ))

    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)


if __name__=="__main__":
    try:
        #trainingTest()
        predictionTest()
    except Exception as e:
        print(str(e))