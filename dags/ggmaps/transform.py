from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from plugins.minio_service import create_client
from plugins.minio_service import create_bucket_minio

def convert_to_dataframe(ti) -> None:
    """
    Converts crawled film data into a Pandas DataFrame and uploads it as a Parquet file to MinIO.
        
    Returns:
        None
    """    
    
    import pandas as pd
    import io

    minio_bucket = 'ggmaps-raw'
    client = create_client()
    create_bucket_minio(client=client,minio_bucket=minio_bucket)

    df_value = ti.xcom_pull(key='crawl',task_ids='crawling.crawl_all')
    df = pd.DataFrame(df_value,columns=['id', 'title', 'links', 'published_date', 'rating', 'quality', 'genre', 'short_description'])
    df.drop_duplicates("title",inplace=True)

    df_parquet = df.to_parquet(index=False)

    client.put_object(
        bucket_name=minio_bucket,
        object_name="ggmaps_request_raw.parquet",
        data=io.BytesIO(df_parquet),
        length=len(df_parquet)
    )

def processing() -> None:
    """
    Processes the raw film data from MinIO, converting the published date and exploding genres.

    Returns:
        None
    """    

    import pandas as pd
    import io
    from io import BytesIO

    minio_bucket = 'ggmaps-raw'
    client = create_client()
    create_bucket_minio(client=client,minio_bucket=minio_bucket)

    raw_ggmaps_object = client.get_object(minio_bucket, "ggmaps_request_raw.parquet")
    df = pd.read_parquet(BytesIO(raw_ggmaps_object.read()))
    new_df = df.copy()
    new_df['published_date'] = pd.to_datetime(new_df['published_date'], format='%d/%m/%Y', errors='coerce')
    processed_df = new_df.explode('genre')
    processed_df['rating'] = processed_df['rating'].astype('float')
    processed_minio_bucket = 'ggmaps-processed'

    create_bucket_minio(client=client,minio_bucket=processed_minio_bucket)

    processed_df_parquet = processed_df.to_parquet(index=False)
    client.put_object(
        bucket_name=processed_minio_bucket,
        object_name="ggmaps_request_processed.parquet",
        data=io.BytesIO(processed_df_parquet),
        length=len(processed_df_parquet)
    )


def processing_tasks():
    with TaskGroup(
            group_id="processing",
            tooltip="processing dataframe"
    ) as group:

        convert_to_dataframe_task = PythonOperator(
            task_id='convert_to_dataframe',
            python_callable=convert_to_dataframe
        )

        processing_task = PythonOperator(
            task_id='processing',
            python_callable=processing
        )

        convert_to_dataframe_task >> processing_task

        return group