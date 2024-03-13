from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "atomic-acrobat-417102"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  #A unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://dataflow_metadata_files/transform_function.js",
        "JSONPath": "gs://dataflow_metadata_files/bigQuery_schema.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "atomic-acrobat-417102:cricket_statistics_dataset.icc_odi_batsman_ranking",
        "inputFilePattern": "gs://bkt-ranking-data/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://dataflow_metadata_files",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)
