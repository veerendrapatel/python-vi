import boto3
import time
from datetime import datetime, timezone
import logging

# AWS clients
rekognition_client = boto3.client("rekognition")
kvs_client = boto3.client("kinesisvideo")
dynamodb = boto3.resource("dynamodb")
analytics_table = dynamodb.Table("BoothAnalyticsTable")
metadata_table = dynamodb.Table("StreamMetadataTable")

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_data_endpoint(stream_name):
    response = kvs_client.get_data_endpoint(
        StreamName=stream_name, APIName="GET_MEDIA_FOR_FRAGMENT_LIST"
    )
    return response["DataEndpoint"]

def get_last_processed_fragment(stream_name):
    """Fetch the last processed fragment or timestamp from the metadata table."""
    try:
        response = metadata_table.get_item(Key={"StreamName": stream_name})
        return response.get("Item", {}).get("LastProcessedFragment", None)
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}")
        return None

def update_last_processed_fragment(stream_name, fragment_number):
    """Update the metadata table with the last processed fragment."""
    try:
        metadata_table.put_item(
            Item={
                "StreamName": stream_name,
                "LastProcessedFragment": fragment_number,
                "LastUpdated": datetime.now(timezone.utc).isoformat(),
            }
        )
        logger.info(f"Updated last processed fragment: {fragment_number}")
    except Exception as e:
        logger.error(f"Error updating metadata: {e}")

def process_video_with_rekognition(stream_name, start_timestamp=None):
    """Process video using Amazon Rekognition, starting from a specific timestamp or the last processed fragment."""
    try:
        last_fragment = get_last_processed_fragment(stream_name)
        start_selector = (
            {"StartSelectorType": "FRAGMENT_NUMBER", "AfterFragmentNumber": last_fragment}
            if last_fragment
            else {"StartSelectorType": "PRODUCER_TIMESTAMP", "StartTimestamp": start_timestamp}
            if start_timestamp
            else {"StartSelectorType": "NOW"}
        )
        
        logger.info(f"Starting Rekognition with selector: {start_selector}")

        response = rekognition_client.start_stream_processor(
            Name="YourRekognitionStreamProcessor",  # Replace with your processor name
            StartSelector=start_selector,
        )
        logger.info(f"Started Rekognition Stream Processor for {stream_name}")

        while True:
            # Pseudocode to fetch analytics results
            analytics, last_fragment_number = get_rekognition_results()  # Replace with actual implementation
            if analytics:
                store_analytics(analytics)
                update_last_processed_fragment(stream_name, last_fragment_number)
            time.sleep(5)  # Poll every 5 seconds
    except Exception as e:
        logger.error(f"Failed to process video with Rekognition: {e}")

def store_analytics(analytics):
    """Store analytics data in DynamoDB."""
    try:
        analytics_table.put_item(Item=analytics)
        logger.info(f"Stored analytics: {analytics}")
    except Exception as e:
        logger.error(f"Failed to store analytics in DynamoDB: {e}")

def get_rekognition_results():
    """Fetches the analysis results from Amazon Rekognition and returns them."""
    try:
        # Example stream processor name (replace with your actual stream processor)
        stream_processor_name = "YourRekognitionStreamProcessor"

        # Fetch the current status of the stream processor
        response = rekognition_client.describe_stream_processor(
            StreamProcessorName=stream_processor_name
        )

        status = response['StreamProcessor']['Status']
        
        if status == 'RUNNING':
            # If the processor is running, attempt to get the results from the processor
            results_response = rekognition_client.get_stream_processor(
                StreamProcessorName=stream_processor_name
            )
            
            # Process the results from the stream processor (e.g., face detection, age, mood, etc.)
            # Assuming you're interested in face analysis and associated data like age, gender, mood
            # Example logic for processing Rekognition results
            analytics = []
            fragments = results_response.get('VideoAnalytics', [])
            
            for fragment in fragments:
                for face in fragment.get('Faces', []):
                    # Extract face metadata (age group, gender, emotion, etc.)
                    analytics.append({
                        'Timestamp': fragment['Timestamp'],
                        'Face': face['Face'],
                        'AgeGroup': face.get('AgeRange', 'Unknown'),
                        'Gender': face.get('Gender', {}).get('Value', 'Unknown'),
                        'Emotions': face.get('Emotions', []),
                    })
            
            # Determine the last fragment number (for updating in metadata)
            last_fragment_number = results_response.get('LastFragmentNumber', None)

            return analytics, last_fragment_number

        elif status == 'FAILED':
            logger.error(f"Stream Processor {stream_processor_name} failed.")
            return [], None
        else:
            logger.info(f"Stream Processor {stream_processor_name} not yet running.")
            return [], None

    except Exception as e:
        logger.error(f"Error getting Rekognition results: {e}")
        return [], None

# Entry point
if __name__ == "__main__":
    STREAM_NAME = "YourKinesisVideoStreamName"  # Replace with your stream name
    START_TIMESTAMP = None  # Set this to a specific timestamp to start from a point in the past
    process_video_with_rekognition(STREAM_NAME, start_timestamp=START_TIMESTAMP)
