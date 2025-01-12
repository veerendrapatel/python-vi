#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the direct video URL using yt-dlp
VIDEO_URL=$(yt-dlp -f best -g "$YOUTUBE_VIDEO_URL")

# Call the Python script to stream video to Kinesis Video Streams
python stream_to_kinesis.py "$VIDEO_URL" "$STREAM_NAME" "$AWS_REGION" "$AWS_PROFILE"
