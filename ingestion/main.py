import dlt
from datetime import date
from youtube_extraction import getYoutubeData
from google.cloud import bigquery
import os

if os.getenv("GITHUB_ACTIONS") == "true":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcloud_credentials.json"


def add_snapshot_date(video_record):
    video_record["snapshot_date"] = date.today()
    return video_record

def youtube_snapshot_pipeline():
    
    pipeline = dlt.pipeline(
        pipeline_name="onepipe_2stacks_pipe",
        destination="bigquery",
        staging="filesystem",
        dataset_name="py_raw"
    )

    youtube_videos = dlt.resource(
        getYoutubeData,
        name="youtube_videos_raw"
    )

    videos_com_snapshot = youtube_videos.add_map(add_snapshot_date)

    videos_com_snapshot.apply_hints(
        table_name="tb_videos", 
        write_disposition={"disposition": "merge", "strategy": "upsert"}, #vendo se o upsert faze com que o raw fique no staging
        primary_key=["id", "snapshot_date"]
    )
    
    print("Início...")
    load_info = pipeline.run(videos_com_snapshot,loader_file_format='jsonl')
    
    print(load_info)
    print("Pipeline executado com sucesso!")

    bigquery.Client().delete_dataset(f"{pipeline.dataset_name}_staging", delete_contents=True, not_found_ok=True)
    print("Tabela staging no destino deletada com sucesso!")


if __name__ == "__main__":
    youtube_snapshot_pipeline()