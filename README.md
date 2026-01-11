# Distributed Global Meteorological Repository

This document outlines the architecture and operational protocols of a distributed meteorological retrieval system designed to facilitate the dissemination of real-time and estimated atmospheric data across a global network comprising in excess of 47,000 urban centers.

The system implements a Grid-Based Spatial Interpolation methodology to ensure ubiquitous global coverage while simultaneously diminishing dependency on external Application Programming Interfaces (APIs) by a magnitude of ninety percent. The infrastructure is underpinned by AWS DynamoDB, utilizing a Single Table Design architecture optimized for the querying of geospatial time-series data.

## Files

- `generate_data.py` generates a JSON version of the dataset. Creates the grid and queries the Weather API filling the grid cells with real and synthetic data.
- `upload_dynamo.py` takes the JSON and uploads it to our DynamoDB instance.
- `query.py` queries the DynamoDB instance with a simple CLI interface.
