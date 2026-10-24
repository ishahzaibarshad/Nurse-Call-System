# Nurse Call System

A hospital monitoring application that receives L7700 device events, stores decoded events, and presents call and room information through a web dashboard.

## Features

- Receives L7700 packets over UDP and decodes device events
- Stores event and room data in MySQL through SQLAlchemy
- Pushes updates to connected clients through a FastAPI WebSocket
- Streams configured RTSP camera feeds to the browser using MJPEG
- Provides pages for the dashboard and active calls, with configuration APIs for hospital areas and cameras

## Stack

Python · FastAPI · MySQL · SQLAlchemy · WebSockets · OpenCV · RTSP · MJPEG · HTML · Tailwind CSS

## Configuration

Configure the database connection, UDP listener, and optional camera sources in the local application configuration. Do not commit production credentials, camera URLs, or patient information. A working MySQL service and the system dependencies for camera streaming are required for the corresponding features.

## Repository contents

The core application is in `server.py`; packet decoding, camera streaming, database models, and configuration are maintained in separate Python modules. The current README contains unrelated Tailwind setup instructions before the project documentation; replace that text with this project-specific overview and add verified run instructions when ready.

