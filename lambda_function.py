#!/usr/bin/python

import sys
import boto3
import time
import random
import string
import os
import json

# Declare environment variables and load from Lambda
s3_bucket_for_logging = os.environ.get("s3_bucket_for_logging")
queue_url = os.environ.get("queue_url")
log_folder_prefix = os.environ.get("log_folder_prefix")
log_object_prefix = os.environ.get("log_object_prefix")
gzip_enabled = os.environ.get("gzip_enabled")

# Cleanup environment variables with bad trailing characters
if log_folder_prefix[-1] == '/':
    log_folder_prefix = log_folder_prefix[0:(len(log_folder_prefix) - 1)]

if log_object_prefix[-1] == '_':
    log_object_prefix = log_object_prefix[0:(len(log_object_prefix) - 1)]


def process_messages():
    processed_msg_count = 0
    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')

    try:
        queue_attrib = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages'
            ]
        )
    except sqs_client.exceptions.QueueDoesNotExist as e:
        print(f"QueueDoesNotExist: {e}")
        raise e

    except Exception as e:
        print(f'Unexpected error from SQS client: {e}')
        raise e

    queue_size = int(queue_attrib['Attributes']['ApproximateNumberOfMessages'])

    if queue_size > 0:
        print(f"Queue Size: {queue_size}")
    else:
        print("Queue is empty. Finalizing and generating report...")
        # Return report content
        return {'queue_size': 0, 'batches': 0, 'processed_messages': 0, 'avg_batch_size': 0}

    if gzip_enabled.lower() in ['true', '1', 't', 'y', 'yes']:
        print("GZIP Compression Enabled.")
        import zlib
        object_ext = 'json.gz'
        object_content_type = 'application/x-gzip'
    else:
        print("GZIP Compression Disabled.")
        object_ext = 'json'
        object_content_type = 'application/json'

    batch = 0
    while batch < queue_size:

        batch += 1
        msg_batch = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )

        if 'Messages' in msg_batch:
            msg_count = len(msg_batch['Messages'])
            print(f"Batch size: {msg_count}")

            for msg in msg_batch['Messages']:
                #print(msg)
                # Prepare S3 key
                log_timestamp = time.gmtime(int(msg['Attributes']['SentTimestamp']) / 1000.)
                s3_log_folder = f"{log_folder_prefix}/{log_timestamp.tm_year}/{log_timestamp.tm_mon}/{log_timestamp.tm_mday}/"
                # Generate 16 character alphanumeric string for object name suffix
                alphanum_key = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
                s3_object_name = f"{log_object_prefix}_{log_timestamp.tm_year}{log_timestamp.tm_mon}{log_timestamp.tm_mday}T{log_timestamp.tm_hour}{log_timestamp.tm_min}Z_{alphanum_key}.{object_ext}"

                # Format message
                this_msg = json.loads(msg['Body'])
                this_msg = this_msg['Message']

                if gzip_enabled.lower() in ['true', '1', 't', 'y', 'yes']:
                    # Compress message body
                    msg_body = zlib.compress(bytes(this_msg, 'utf-8'))
                else:
                    msg_body = this_msg

                try:
                    # Write log file to S3
                    s3_client.put_object(
                        Body=msg_body,
                        Bucket=s3_bucket_for_logging,
                        Key=s3_log_folder + s3_object_name,
                        ContentType=object_content_type
                    )
                except Exception as e:
                    print(f'Unexpected error from S3 client: {e}')
                    raise e

                try:
                    # Delete processed message from queue
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                except Exception as e:
                    print(f'Unexpected error from SQS client: {e}')
                    raise e

            # Add message total of this batch to report
            processed_msg_count += len(msg_batch['Messages'])

        else:
            print("Batch size of 0. Finalizing and generating report...")
            break

    # Return report content
    return {'queue_size': queue_size, 'batches': batch, 'processed_messages': processed_msg_count, 'avg_batch_size': round((processed_msg_count / batch), 3)}


def main():
    report = process_messages()
    print(report)


def lambda_handler(event, context):
    report = process_messages()
    return {
        'report': report
    }


if __name__ == '__main__': main()
