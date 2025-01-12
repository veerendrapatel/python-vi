import boto3
import time
import io
import tempfile
from botocore.exceptions import ClientError
import logging
import ffmpeg
from pymkv import MKVFile
# Set up logging
# logging.basicConfig(level=logging.DEBUG, filename='video_processing.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
# Initialize the Kinesis Video Streams client to fetch media
kvs_client = boto3.client('kinesis-video-archived-media', region_name='eu-west-1')

# Initialize the Kinesis Video Streams client to get stream information
kinesis_video_client = boto3.client('kinesisvideo', region_name='eu-west-1')
kinesis_video_media_client = boto3.client('kinesis-video-media', region_name='eu-west-1')
# Specify the stream name
stream_name = 'video-stream-1'

# Get the stream's ARN
def get_stream_arn(stream_name):
    try:
        response = kinesis_video_client.describe_stream(StreamName=stream_name)
        return response['StreamInfo']['StreamARN']
    except ClientError as e:
        print(f"Error getting stream ARN: {e}")
        return None

def get_video_fragment(stream_arn):
    try:
        # Start selector as 'NOW' will get the most recent fragment
        start_selector = {
            'StartSelectorType': 'NOW'
        }

        # Get media (video fragments)
        response = kinesis_video_media_client.get_media(
            StreamARN=stream_arn,
            StartSelector=start_selector
        )
        print(f"Received response {response}")

    except ClientError as e:
        print(f"Error retrieving fragment: {e}")

def main():
    stream_arn = get_stream_arn(stream_name)
    if stream_arn:
        print(f"Stream ARN: {stream_arn}")
        while True:
            # Get fragments continuously
            get_video_fragment(stream_arn)
            time.sleep(5)  # Adjust time interval as needed

if __name__ == '__main__':
    main()
