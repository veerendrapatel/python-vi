import boto3
import time
import json
import uuid
from botocore.exceptions import ClientError

# Initialize clients
kinesis_video_client = boto3.client('kinesisvideo', region_name='eu-west-1')
kinesis_video_media_client = None
rekognition_client = boto3.client('rekognition', region_name='eu-west-1')
dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')

# DynamoDB table to store the processed data
DYNAMODB_TABLE = 'FragmentAnalyticsData'

# Function to get Kinesis Video Stream media client
def get_media_client(stream_name):
    global kinesis_video_media_client
    try:
        response = kinesis_video_client.get_data_endpoint(
            StreamName=stream_name,
            APIName='GET_MEDIA'
        )
        endpoint_url = response['DataEndpoint']
        kinesis_video_media_client = boto3.client('kinesis-video-media', endpoint_url=endpoint_url, region_name='eu-west-1')
    except Exception as e:
        print(f"Error getting media client: {e}")

# Function to retrieve fragments from the Kinesis Video Stream
def get_fragment_number_and_data(stream_name, start_selector=None):
    """
    Retrieves the fragment number and data from the Kinesis Video Stream.

    Args:
        stream_name (str): The name of the Kinesis Video Stream.
        start_selector (dict): The starting selector for retrieving the fragment.

    Returns:
        tuple: A tuple containing the fragment number and fragment data.
    """
    global kinesis_video_media_client
    try:
        print(f"Starting to retrieve fragment data for stream: {stream_name}")

        # Check and initialize the Kinesis Video Media client
        if not kinesis_video_media_client:
            print("Kinesis Video Media client not initialized. Initializing now...")
            get_media_client(stream_name)
        else:
            print("Kinesis Video Media client already initialized.")
        # Get the latest fragment number using the list_fragments method
        latest_fragment_number = list_fragments(stream_name)

        if latest_fragment_number is None:
            print("No fragment found.")
            return None, None
        # Set default start selector if not provided
        if start_selector is None:
            start_selector = {'StartSelectorType': 'NOW'}  # Default to start now
            print("No start selector provided. Defaulting to start now.")

        print(f"Using start selector: {start_selector}")

        # Call the GetMedia API to retrieve fragment data
        response = kinesis_video_media_client.get_media(
            StreamName=stream_name,
            StartSelector=start_selector
        )
        print("Successfully called get_media API.")

        # Debug response structure
        print(f"GetMedia response: {response}")

        # Extract fragment number and payload from the response
        fragment_number = None
        fragment_data = response.get('Payload')

        # Debugging the fragment_number in case it's embedded within Payload metadata
        # Normally the FragmentNumber is part of the stream's metadata or associated with the Payload data
        if 'FragmentNumber' in response:
            fragment_number = response['FragmentNumber']
            print(f"Retrieved FragmentNumber: {fragment_number}")
        else:
            print("FragmentNumber not found in the response.")

        if fragment_data:
            print("Fragment data retrieved successfully.")
        else:
            print("Fragment data not found in the response.")

        # In case the fragment number was not directly found, extract it from stream metadata or sequence
        if not fragment_number:
            print("Trying to extract fragment number from Payload metadata...")

            # If possible, here you can extract metadata from the Payload stream or use sequence numbers from the video stream
            # Additional code logic here, depending on how the video stream is structured and how you handle stream metadata

        return fragment_number, fragment_data

    except Exception as e:
        print(f"Error retrieving fragment: {e}")
        return None, None

# Function to list fragments and get the latest fragment number
def list_fragments(stream_name):
    try:
        # Get the list of fragments in the stream
        response = kinesis_video_media_client.list_fragments(
            StreamName=stream_name,
            MaxResults=5  # Adjust based on how many fragments you want to check
        )
        print(f"ListFragments response: {response}")
        
        # Retrieve the fragment numbers and other metadata
        fragments = response.get('Fragments', [])
        if fragments:
            # Process the fragment metadata to get the latest fragment number
            latest_fragment = max(fragments, key=lambda x: int(x['Timestamp']))
            fragment_number = latest_fragment['FragmentNumber']
            print(f"Latest fragment number: {fragment_number}")
            return fragment_number
        else:
            print("No fragments found.")
            return None
    except Exception as e:
        print(f"Error listing fragments: {e}")
        return None


# Function to call Rekognition to process video data (e.g., face analysis, labels)
def analyze_video_with_rekognition(video_stream):
    try:
        response = rekognition_client.start_faces_detection(
            Video={'Bytes': video_stream},
            # NotificationChannel={'RoleArn': 'arn:aws:iam::your-account-id:role/rekognition-role', 'SNSTopicArn': 'arn:aws:sns:us-east-1:your-account-id:rekognition-topic'}
        )
        job_id = response['JobId']
        return job_id
    except ClientError as e:
        print(f"Error processing with Rekognition: {e}")
        return None

# Function to get the Rekognition analysis results
def get_rekognition_results(job_id):
    try:
        response = rekognition_client.get_faces_detection(JobId=job_id)
        return response['Faces']
    except ClientError as e:
        print(f"Error getting Rekognition results: {e}")
        return None

# Function to store analytics data in DynamoDB
def store_analytics_data(fragment_number, analytics_data):
    try:
        # Format analytics data for DynamoDB
        item = {
            'SK': {'S': str(fragment_number)},
            'AnalyticsData': {'S': json.dumps(analytics_data)},
            'PK': {'S': str(int(time.time()))}
        }

        # Store the data in DynamoDB
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item=item
        )
        print(f"Stored analytics data for fragment: {fragment_number}")
    except ClientError as e:
        print(f"Error storing analytics data in DynamoDB: {e}")

# Function to track last processed fragment number and handle reprocessing
def get_last_processed_fragment():
    """
    Retrieves the last processed fragment from the DynamoDB table.

    Returns:
        str: The last processed fragment number, or None if not found.
    """
    try:
        # Query DynamoDB to get the single item with PK='ProcessedFragments' and SK='ProcessedFragments'
        response = dynamodb_client.query(
            TableName=DYNAMODB_TABLE,
            KeyConditionExpression="PK = :pk AND SK = :sk",
            ExpressionAttributeValues={
                ":pk": {"S": "ProcessedFragments"},
                ":sk": {"S": "ProcessedFragments"}
            },
            Limit=1  # Limit to one item (though only one exists by design)
        )

        # If the item exists, return the FragmentNumber
        if response.get('Items'):
            last_fragment_number = response['Items'][0]['FragmentNumber']['S']
            return last_fragment_number

        print("No processed fragments found in DynamoDB.")
        return None

    except ClientError as e:
        print(f"Error retrieving last processed fragment: {e}")
        return None


# Function to process video stream and store analytics data
def process_video_stream(stream_name, start_timestamp=None):
    last_processed_fragment = get_last_processed_fragment() if not start_timestamp else None
    print(f"last_processed_fragment: {last_processed_fragment}")

    # Define start selector based on start_timestamp or last processed fragment
    start_selector = None
    if start_timestamp:
        start_selector = {
            'StartSelectorType': 'PRODUCER_TIMESTAMP',
            'StartTimestamp': float(start_timestamp)  # Timestamp should be in seconds as a float
        }
    elif last_processed_fragment:
        start_selector = {
            'StartSelectorType': 'FRAGMENT_NUMBER',
            'FragmentNumber': str(last_processed_fragment)
        }
    else:
        start_selector = {'StartSelectorType': 'NOW'}

    # Continuously process the stream
    while True:
        # Retrieve the fragment and its data
        fragment_number, fragment_data = get_fragment_number_and_data(stream_name, start_selector)
        
        if fragment_data:
            # Analyze the fragment with Rekognition
            job_id = analyze_video_with_rekognition(fragment_data)
            if job_id:
                print(f"Analyzing fragment: {fragment_number} with Rekognition Job ID: {job_id}")

                # Wait for the Rekognition results
                time.sleep(10)  # Adjust the sleep time for your processing
                results = get_rekognition_results(job_id)

                # Extract analytics data (e.g., age, sex, mood, etc.)
                analytics_data = extract_analytics_data(results)

                # Store the analytics data in DynamoDB
                store_analytics_data(fragment_number, analytics_data)
                update_last_processed_fragment(fragment_number)
                # Update start_selector to the next fragment for continuous processing
                start_selector = {'StartSelectorType': 'FRAGMENT_NUMBER', 'FragmentNumber': str(int(fragment_number) + 1)}

        time.sleep(5)  # Adjust the polling interval as necessary

# Example function to extract analytics data (simplified)
def extract_analytics_data(results):
    analytics_data = {
        'age_group': 'unknown',
        'sex': 'unknown',
        'mood': 'unknown'
    }
    for face in results:
        if 'AgeRange' in face:
            age_range = face['AgeRange']
            analytics_data['age_group'] = f"{age_range['Low']}-{age_range['High']}"
        if 'Gender' in face:
            analytics_data['sex'] = face['Gender']['Value']
        if 'Emotions' in face:
            for emotion in face['Emotions']:
                if emotion['Type'] == 'HAPPY':
                    analytics_data['mood'] = 'Happy'
    return analytics_data

def update_last_processed_fragment(fragment_number):
    """
    Updates the last processed fragment information in DynamoDB.

    Parameters:
        fragment_number (str): The fragment number to update in the table.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        # Current timestamp
        current_timestamp = int(time.time())

        # Item to update in DynamoDB
        item = {
            'PK': {'S': 'ProcessedFragments'},
            'SK': {'S': 'ProcessedFragments'},
            'FragmentNumber': {'S': fragment_number},
            'Timestamp': {'N': str(current_timestamp)}
        }

        # Put the item into the table
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item=item
        )
        print(f"Successfully updated fragment number {fragment_number} in DynamoDB.")
        return True

    except ClientError as e:
        print(f"Error updating fragment number in DynamoDB: {e}")
        return False
    
# Run the script
if __name__ == "__main__":
    stream_name = 'video-stream-1'
    process_video_stream(stream_name, start_timestamp=None)
