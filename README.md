# Distributed Global Meteorological Repository

This document outlines the architecture and operational protocols of a distributed meteorological retrieval system designed to facilitate the dissemination of real-time and estimated atmospheric data across a global network comprising in excess of 47,000 urban centers.

The system implements a Grid-Based Spatial Interpolation methodology to ensure ubiquitous global coverage while simultaneously diminishing dependency on external Application Programming Interfaces (APIs) by a magnitude of ninety percent. The infrastructure is underpinned by AWS DynamoDB, utilizing a Single Table Design architecture optimized for the querying of geospatial time-series data.

## I. Operational Prerequisites and Initialization

### 1. System Requirements

The successful deployment of this system is contingent upon the availability of the following:
- Python 3.8+ execution environment.
- AWS Credentials (Compatible with Standard and AWS Academy/VocLabs accounts).
- pip (Python package manager).

### 2. Installation Protocol

The acquisition of the repository and the subsequent installation of necessary dependencies shall be executed via the following directive:
```
pip install boto3 pandas requests scipy tqdm
```

3. AWS Credential Configuration

It is imperative that valid AWS credentials be established to facilitate the operation of the upload mechanism and the query interface.

Subsection A: Command Line Interface (CLI) Configuration
Configuration of the CLI is to be performed by executing the initialization command and adhering to the subsequent prompts regarding Access Key identifiers and Secret Access Keys.
```
aws configure
```

- AWS Access Key ID: [Insert from AWS Console]
- AWS Secret Access Key: [Insert from AWS Console]
- Default region name: us-east-1 (Mandatory for this environment)
- Default output format: json

Subsection B: Session Token Configuration (Restricted Environments)
For users operating within restricted environments, such as AWS Academy, the standard configuration utility omits the session token parameter by default. Consequently, the manual insertion of the aws_session_token into the credentials configuration file is required.

1. Access the credentials file:
- Unix-based Systems: `~/.aws/credentials`

2. Append the aws_session_token parameter under the [default] profile:
```
[default]
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = [INSERT_TOKEN_STRING_HERE]
```

## II. Pipeline Execution Protocols

The functionality of the system is segmented into three distinct operational phases. Phases I and II are typically executed a single time for database construction, whereas Phase III constitutes the recurring query operation.

### Phase I: Data Generation and Interpolation

This procedure instantiates the global grid reference system, acquires meteorological data for ten percent of the grid ('Anchor' nodes), and applies mathematical interpolation to derive data for the remaining ninety percent. The execution of generate_data.py initiates this process. It should be noted that a checkpointing mechanism is employed to mitigate interruptions caused by API rate limiting.
- Input Source: worldcities.csv
- Output Artifact: world_weather_final.json
```
python3 generate_data.py
```

Note: The execution of this process requires approximately 20 to 30 minutes due to imposed API rate limits.

**Phase II: Cloud Infrastructure Upload**
The data generated in Phase I is transmitted to the AWS DynamoDB instance via the execution of the upload script. A "Blind Upload" strategy is utilized to circumvent restrictive permission policies inherent in certain laboratory environments.

Input Source: world_weather_final.json

Target Destination: AWS DynamoDB Table (WeatherForecast)

#### Recommended for local execution environments
python laptop_uploader.py

#### Alternative upload utility
python upload_dynamo.py


Financial Disclaimer: The uploading of approximately one million records in DynamoDB On-Demand mode incurs a financial cost proportional to the volume of write operations, estimated between $1.50 and $2.50 USD.

### Phase III: Query Interface

The retrieval of meteorological data is facilitated through the command-line interface.
```
python query_weather.py
```

Functionality:
- Nomenclature Disambiguation: The system distinguishes between distinct urban entities sharing identical nomenclature (e.g., Barcelona, Spain versus Barcelona, Venezuela).
- Coordinate-Based Retrieval: Mathematical calculation of Grid IDs is performed based on provided Latitude and Longitude coordinates.
- Data Provenance: The output explicitly indicates whether the data is derived from a sensor (Anchor) or via estimation (Interpolation).

## III. Repository Structure

The following table enumerates the constituent files of the repository and their respective functions:

File

Description

generate_data.py

The Extract, Transform, Load (ETL) pipeline. Ingests CSV data, interacts with the Open-Meteo API, and executes KD-Tree interpolation.

laptop_uploader.py

A high-performance batch upload utility utilizing low-level Boto3 clients to bypass permission checks.

query_weather.py

The user-facing Command Line Interface application for database querying.

viz.py

(Optional) Generates an HTML representation visualizing the grid coverage.

worldcities.csv

The source dataset containing global urban location data.

IV. Architectural Specifications

Database Infrastructure: AWS DynamoDB configured in On-Demand Mode.

Spatial Partitioning: The globe is divided into grid squares measuring 0.18° by 0.18° (approximately 20km).

Key Schema:

Partition Key (PK): GridID (e.g., GRID#776#198)

Sort Key (SK): Timestamp

Indexing Strategy: A Global Secondary Index (GSI) is maintained on the LocationName attribute to facilitate O(1) complexity lookups.

## V. Diagnostic and Remediation Procedures

Case: "ResourceNotFoundException" during upload
In the event of a ResourceNotFoundException, it may be inferred that the DynamoDB table has not been initialized. The manual execution of the following table creation command is prescribed:
```
aws dynamodb create-table \
    --table-name WeatherForecast \
    --attribute-definitions AttributeName=GridID,AttributeType=S AttributeName=Timestamp,AttributeType=S AttributeName=LocationName,AttributeType=S \
    --key-schema AttributeName=GridID,KeyType=HASH AttributeName=Timestamp,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes "IndexName=CityNameIndex,KeySchema=[{AttributeName=LocationName,KeyType=HASH},{AttributeName=Timestamp,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
    --region us-east-1
```

Case: "AccessDeniedException" during query
This error typically indicates the expiration of ephemeral credentials. The rectification procedure involves the acquisition of new credentials from the AWS Academy console and the subsequent updating of the ~/.aws/credentials file.
