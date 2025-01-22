import os
import cv2
import boto3
import json
import uuid
from datetime import datetime

# Initialize AWS resources
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')

# DynamoDB table for analytics data
table_name = "ExhibitionAnalytics"  # Replace with your table name
table = dynamodb.Table(table_name)

# DynamoDB table for checkpoints
checkpoint_table_name = "ExhibitionCheckpoints"  # Replace with your checkpoint table name
checkpoint_table = dynamodb.Table(checkpoint_table_name)

# Directory paths for files
VIDEO_DIR = "./videos"  # Directory containing .mkv files
IMAGE_DIR = "./images"  # Directory containing .jpg files

# DynamoDB table for visitor tracking
visitor_table_name = "VisitorTracking"  # Replace with your visitor table name
visitor_table = dynamodb.Table(visitor_table_name)

def load_checkpoint():
    """Load the processing checkpoint from DynamoDB."""
    try:
        response = checkpoint_table.get_item(Key={"id": "checkpoint"})
        return response.get("Item", {"videos": [], "images": []})
    except Exception as e:
        print(f"Error loading checkpoint: {e}")
        return {"videos": [], "images": []}

def save_checkpoint(checkpoint):
    """Save the processing checkpoint to DynamoDB."""
    try:
        checkpoint_table.put_item(Item={"id": "checkpoint", **checkpoint})
    except Exception as e:
        print(f"Error saving checkpoint: {e}")

def process_video(file_path):
    """Process a video file for visitor analytics."""
    print(f"Processing video: {file_path}")
    cap = cv2.VideoCapture(file_path)
    frame_rate = int(cap.get(cv2.CAP_PROP_FPS))
    frame_interval = frame_rate * 10  # Process every 10 seconds

    frame_count = 0
    video_id = str(uuid.uuid4())

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        if frame_count % frame_interval == 0:
            # Save frame as image for analysis
            frame_path = f"frame_{video_id}_{frame_count}.jpg"
            cv2.imwrite(frame_path, frame)
            analyze_frame(frame_path, video_id)
            os.remove(frame_path)  # Clean up frame

        frame_count += 1

    cap.release()

def process_image(file_path):
    """Process a single image for demographics analytics."""
    print(f"Processing image: {file_path}")
    analyze_frame(file_path)

def analyze_frame(image_path, video_id=None):
    """Analyze a frame or image using AWS Rekognition and remove PII."""
    try:
        with open(image_path, 'rb') as image_file:
            response = rekognition.detect_faces(
                Image={"Bytes": image_file.read()},
                Attributes=["ALL"]
            )

        # Aggregate demographic data
        age_ranges = []
        gender_counts = {"Male": 0, "Female": 0, "Unknown": 0}
        emotion_counts = {}

        for face in response.get("FaceDetails", []):
            # Aggregate age ranges
            if "AgeRange" in face:
                age_ranges.append(face["AgeRange"])

            # Count gender distribution
            gender = face.get("Gender", {}).get("Value", "Unknown")
            gender_counts[gender] += 1

            # Count emotions
            for emotion in face.get("Emotions", []):
                emotion_name = emotion.get("Type")
                if emotion_name:
                    emotion_counts[emotion_name] = emotion_counts.get(emotion_name, 0) + 1

        # Compute overall age range (min and max)
        overall_age_range = {
            "Min": min(age["Low"] for age in age_ranges) if age_ranges else None,
            "Max": max(age["High"] for age in age_ranges) if age_ranges else None,
        }

        # Analyze foot impressions and visitor tracking
        foot_impressions = len(response.get("FaceDetails", []))
        visitor_id = str(uuid.uuid4())  # Generate unique ID for each visitor (replace with actual logic if needed)
        timestamp = datetime.utcnow().isoformat()

        # Update visitor tracking data
        visitor_data = visitor_table.get_item(Key={"visitor_id": visitor_id}).get("Item")
        if visitor_data:
            visit_count = visitor_data.get("visit_count", 0) + 1
            dwell_time = visitor_data.get("dwell_time", 0) + 10  # Assuming 10 seconds per frame
        else:
            visit_count = 1
            dwell_time = 10

        visitor_table.put_item(Item={
            "visitor_id": visitor_id,
            "last_seen": timestamp,
            "visit_count": visit_count,
            "dwell_time": dwell_time
        })

        # Sanitize and save data
        data = {
            "id": str(uuid.uuid4()),
            "timestamp": timestamp,
            "video_id": video_id,
            "demographics": {
                "overall_age_range": overall_age_range,
                "gender_distribution": gender_counts,
                "emotion_counts": emotion_counts,
            },
            "foot_impressions": foot_impressions,
            "visitor_id": visitor_id,
            "visit_count": visit_count,
            "dwell_time": dwell_time,
        }

        # Store in DynamoDB
        table.put_item(Item=data)
    except Exception as e:
        print(f"Error analyzing frame: {e}")

def main():
    checkpoint = load_checkpoint()
    processed_videos = set(checkpoint.get("videos", []))
    processed_images = set(checkpoint.get("images", []))

    # Process videos
    for video_file in os.listdir(VIDEO_DIR):
        video_path = os.path.join(VIDEO_DIR, video_file)
        if video_file.endswith(".mkv") and video_path not in processed_videos:
            try:
                process_video(video_path)
                processed_videos.add(video_path)
                checkpoint["videos"] = list(processed_videos)
                save_checkpoint(checkpoint)
            except Exception as e:
                print(f"Error processing video {video_file}: {e}")

    # Process images
    for image_file in os.listdir(IMAGE_DIR):
        image_path = os.path.join(IMAGE_DIR, image_file)
        if image_file.endswith(".jpg") and image_path not in processed_images:
            try:
                process_image(image_path)
                processed_images.add(image_path)
                checkpoint["images"] = list(processed_images)
                save_checkpoint(checkpoint)
            except Exception as e:
                print(f"Error processing image {image_file}: {e}")

if __name__ == "__main__":
    main()
