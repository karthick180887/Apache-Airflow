import boto3
import json
import time
from datetime import datetime

# Initialize clients
kinesis = boto3.client('kinesis', region_name='your-region')  # e.g., 'us-east-1'
s3 = boto3.client('s3', region_name='your-region')

# Configuration
STREAM_NAME = 'StudentDataStream'
S3_BUCKET = 'your-bucket-name'
SHARD_ITERATOR_TYPE = 'TRIM_HORIZON'  # Start with oldest record
MAX_RECORDS = 100  # Max records per batch
CHECKPOINT_FILE = 'kinesis_checkpoint.txt'  # To track processed records

def get_shard_ids():
    """Get all shard IDs for the stream"""
    response = kinesis.describe_stream(StreamName=STREAM_NAME)
    return [shard['ShardId'] for shard in response['StreamDescription']['Shards']]

def get_checkpoint(shard_id):
    """Get the last processed sequence number for a shard"""
    try:
        with open(f"{shard_id}_{CHECKPOINT_FILE}", 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def save_checkpoint(shard_id, sequence_number):
    """Save the last processed sequence number for a shard"""
    with open(f"{shard_id}_{CHECKPOINT_FILE}", 'w') as f:
        f.write(sequence_number)

def process_records(records):
    """Process and format records for S3"""
    formatted_records = []
    for record in records:
        try:
            # Decode the data and add metadata
            data = json.loads(record['Data'].decode('utf-8'))
            formatted_records.append({
                'data': data,
                'metadata': {
                    'approximate_arrival_timestamp': record['ApproximateArrivalTimestamp'].isoformat(),
                    'sequence_number': record['SequenceNumber'],
                    'partition_key': record['PartitionKey']
                }
            })
        except Exception as e:
            print(f"Error processing record: {e}")
    return formatted_records

def write_to_s3(data, shard_id):
    """Write processed data to S3"""
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    key = f"kinesis-data/{shard_id}/{timestamp}.json"
    
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(data, indent=2)
        )
        print(f"Successfully wrote {len(data)} records to s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"Error writing to S3: {e}")

def process_shard(shard_id):
    """Process records from a single shard"""
    last_sequence_number = get_checkpoint(shard_id)
    
    if last_sequence_number:
        shard_iterator = kinesis.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType='AFTER_SEQUENCE_NUMBER',
            StartingSequenceNumber=last_sequence_number
        )['ShardIterator']
    else:
        shard_iterator = kinesis.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType=SHARD_ITERATOR_TYPE
        )['ShardIterator']
    
    while True:
        try:
            response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=MAX_RECORDS
            )
            
            if not response['Records']:
                print(f"No new records in shard {shard_id}. Waiting...")
                time.sleep(5)
                continue
            
            # Process and store records
            processed_records = process_records(response['Records'])
            write_to_s3(processed_records, shard_id)
            
            # Update checkpoint
            last_sequence_number = response['Records'][-1]['SequenceNumber']
            save_checkpoint(shard_id, last_sequence_number)
            
            # Get next iterator
            shard_iterator = response['NextShardIterator']
            time.sleep(1)  # Avoid throttling
            
        except Exception as e:
            print(f"Error processing shard {shard_id}: {e}")
            time.sleep(5)

def main():
    shard_ids = get_shard_ids()
    print(f"Found shards: {shard_ids}")
    
    # Process each shard (in production, you might want to parallelize this)
    for shard_id in shard_ids:
        print(f"Starting to process shard: {shard_id}")
        process_shard(shard_id)

if __name__ == "__main__":
    main()