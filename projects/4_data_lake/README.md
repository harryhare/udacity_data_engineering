## Udacity data engineer project 4  spark

### Intro

This project use spark to do the elt job for the same Sparkify data we use in the previous projects.
This project aim to fetch data from public S3 bucket, and after processing the data save the result back to private S3 bucket. 

**I have some issue to save file to s3, so if the script running locally, the output will be saved locally**

### How to run

* run locally

    There is a configuration `runlocal` in dl.cfg. If it's set to true, then the code will read from local files `.data/` and save results locally `.output/`.
    
    ```bash
    python3 etl.py
    ```
    
* run on emr
    
    ```bash
    spark-submit --master yarn etl.py
    ```