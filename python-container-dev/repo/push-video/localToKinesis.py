import boto3
import requests
import os
import time

def main():
    # Set AWS profile and region
    os.environ['AWS_PROFILE'] = '577411803844_terraform'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-1'

    # Define parameters
    stream_name = 'video-stream-1'
    video_file_path = 'video1.mp4'

    # Create Kinesis Video client
    kvs_client = boto3.client('kinesisvideo', region_name=os.environ['AWS_DEFAULT_REGION'])

    # Get the endpoint for the Kinesis Video Stream
    endpoint_response = kvs_client.get_data_endpoint(StreamName=stream_name, APIName='PUT_MEDIA')
    endpoint = endpoint_response['DataEndpoint']
    
    # Log the endpoint for debugging
    print(f"Endpoint URL for PUT_MEDIA: {endpoint}")

    # Start streaming video
    with open(video_file_path, 'rb') as video_file:
        chunk_size = 1024 * 1024  # 1 MB
        while True:
            data = video_file.read(chunk_size)
            if not data:
                break  # End of file
            
            # Sending PUT request to the Kinesis Video endpoint
            response = requests.put(endpoint, data=data, headers={'Content-Type': 'application/octet-stream'})
            if response.status_code != 200:
                print(f"Failed to put media: {response.status_code} - {response.text}")
            else:
                print("Successfully put media chunk.")

            time.sleep(0.1)  # Control flow of data

    print("Streaming completed.")

if __name__ == "__main__":
    main()
