import json
import boto3
import botocore
import os
import time

# Initialize the S3 and Kinesis Video clients
s3_client = boto3.client("s3")
kvs_client = boto3.client("kinesisvideo")

def lambda_handler(event, context):
    # Input parameters
    s3_bucket = "veer-temp-videos"  # Replace with your S3 bucket name
    s3_key = "video1.mp4"            # Replace with your S3 video file key
    kinesis_stream_name = "video-stream-1"  # Replace with your Kinesis stream name
    
    # Download the MP4 file from S3
    try:
        local_file_path = "/tmp/video.mp4"  # Temp storage for Lambda environment
        s3_client.download_file(s3_bucket, s3_key, local_file_path)
        print(f"Downloaded {s3_key} from S3 bucket {s3_bucket}")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to download video from S3: {e}")
        return {"statusCode": 500, "body": json.dumps("Error downloading video from S3")}

    # Get the Kinesis Video Stream endpoint
    try:
        response = kvs_client.get_data_endpoint(
            StreamName=kinesis_stream_name,
            APIName="PUT_MEDIA"
        )
        endpoint_url = response["DataEndpoint"]
        print(f"Endpoint URL for Kinesis Video Stream: {endpoint_url}")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to get Kinesis Video Stream endpoint: {e}")
        return {"statusCode": 500, "body": json.dumps("Error getting Kinesis endpoint")}

    # Initialize the Kinesis Video Media client
    kvs_media_client = boto3.client("kinesis-video-media", endpoint_url=endpoint_url)

    # Stream the video to Kinesis Video Streams
    try:
        with open(local_file_path, "rb") as video_file:
            # Create a media stream request
            response = kvs_media_client.put_media(
                StreamName=kinesis_stream_name,
                Payload=video_file,
                ContentType="video/mp4",
                # Add additional parameters if needed
                FragmentSelector={
                    'FragmentSelectorType': 'PRODUCER_TIMESTAMP'
                }
            )
        
        print(f"Successfully streamed video to Kinesis Video Stream {kinesis_stream_name}")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to stream video to Kinesis Video Stream: {e}")
        return {"statusCode": 500, "body": json.dumps("Error streaming video to Kinesis")}

    return {
        "statusCode": 200,
        "body": json.dumps("Video successfully streamed to Kinesis Video Stream")
    }
