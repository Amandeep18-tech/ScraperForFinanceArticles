import json
import boto3
import logging
import traceback

import iteration_utilities as utils

from pathlib import Path


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(Path(__file__).stem)

class SQSUtils:
    def __init__(self,queue_name, access_key_id = None, secret_access_key_id = None):
        if access_key_id and secret_access_key_id:
            self.sqs_client=boto3.client('sqs',aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key_id)
        else:
            self.sqs_client=boto3.client('sqs')
        self.queue_url=self.sqs_client.get_queue_url(QueueName=queue_name)
    
    def batch_send_to_sqs(self,articles):
        batch_generator = utils.grouper(articles, 5)
        for batch in batch_generator:
            try:
                response = self.sqs_client.send_message_batch(QueueUrl=self.queue_url,Entries=batch)
                failures = response.get('Failed')
                if failures:
                    logger.error(f"SQS Response: {response}")
                    raise Exception(f"SQS Queue {self.queue_name} did no accept data {failures}")
                else:
                    logger.info(f"SQS Response: {response}")
            except Exception as e:
                for message in batch:
                    try:
                        response = self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))
                    except Exception as e:
                        logger.error("Error : {0}\nException : {1}".format(e,traceback.format_exc()))
                        logger.info("Failed Message: {0}".format(json.dumps(message)))
                    