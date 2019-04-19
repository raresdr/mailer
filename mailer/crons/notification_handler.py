import os, sys
# workaround for a parent directory import to not fail
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import json
import boto3
import datetime
from base import Base
from queries import INSERT_NOTIFICATIONS_SQL

class NotificationHandler(Base):
    """Class for handling bounce and complaint notifications."""

    def __init__(self):
        """Using parent's init method for initializing the scheduler object with conf, logger, db connection handler."""
        super().__init__()

        # initialize aws sqs client & queue_url
        self.sqs = boto3.client(
            'sqs',
            region_name=self.conf['aws']['region'],
            aws_access_key_id=self.conf['aws']['aws_access_key_id'],
            aws_secret_access_key=self.conf['aws']['aws_secret_access_key']
        )
        self.queue_url = self.sqs.get_queue_url(QueueName=self.conf['aws']['sqs']['queue_name'])['QueueUrl']
        self.notifications_processed = 0

    def process_notifications(self):
        """Method for processing SNS notifications by listening to SQS."""

        # retrieve batches(max 10) of messages from SQS
        sqs_receive_response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.conf['aws']['sqs']['max_message_no']
        )

        self.log.info('Started processing SNS notifications...')
        while sqs_receive_response and 'Messages' in sqs_receive_response and sqs_receive_response['Messages']:
            notifications = self.parse_messages(messages=sqs_receive_response['Messages'])
            body_notifications = [notification['body'] for notification in notifications]

            try:
                self.insert_notifications(notifications=body_notifications)
                self.notifications_processed += len(notifications)

                # build message list for SQS delete call
                message_entries = [
                    {
                        'Id': notification['message_id'],
                        'ReceiptHandle': notification['receipt_handle']
                    } for notification in notifications
                ]

                sqs_delete_response = self.sqs.delete_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=message_entries
                )

                if not sqs_delete_response:
                    raise Exception('Notifications inserted but no response on sqs delete batch call.'
                        'This will most probably result in duplicate notifications in db.')
                elif sqs_delete_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception('Notifications inserted but sqs delete batch response status not 200.'
                        'This will most probably result in duplicate notifications in db.')
                elif 'Successful' not in sqs_delete_response or len(sqs_delete_response['Successful']) != len(notifications):
                    raise Exception('Notifications inserted but sqs delete batch call was not successful for every notification.'
                        'This will most probably result in duplicate notifications in db.')
            except Exception as e:
                self.log.exception(e)
            finally:
                if not self.notifications_processed % 100:
                    self.log.info('Processed %d notifications so far...' % self.notifications_processed)

            sqs_receive_response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.conf['aws']['sqs']['max_message_no']
            )
        self.log.info('No more notifications found. Total notifications processed: %d' % self.notifications_processed)

    def parse_messages(self, messages):
        if not messages:
            return

        notifications = []
        for message in messages:
            notification = {'message_id': message['MessageId'], 'receipt_handle': message['ReceiptHandle']}
            body_message = json.loads(json.loads(message['Body'])['Message'])
            body = {}

            body['notification_type'] = body_message['notificationType'].lower()
            if body['notification_type'] == 'bounce':
                body['type'] = body_message['bounce']['bounceType']
                body['subtype'] = body_message['bounce']['bounceSubType']
                body['recipient'] = body_message['bounce']['bouncedRecipients'][0]['emailAddress']
                body['recipient_diagnostic'] = body_message['bounce']['bouncedRecipients'][0]['diagnosticCode']
            elif body['notification_type'] == 'complaint':
                body['type'] = body_message['complaint']['complaintFeedbackType']
                body['subtype'] = None
                body['recipient'] = body_message['complaint']['complainedRecipients'][0]['emailAddress']
                body['recipient_diagnostic'] = None
            # format datetime for db
            body['notification_received_date'] = datetime.datetime \
            .strptime(body_message[body['notification_type']]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ') \
            .strftime('%Y-%m-%d %H:%M:%S')

            # retrieve campaign and end user id specific headers
            headers = [header for header in body_message['mail']['headers'] if header['name'] in ['CAMPAIGN_ID', 'END_USER_ID']]
            for specific_header in headers:
                body[specific_header['name'].lower()] = int(specific_header['value'])
            notification['body'] = body
            notifications.append(notification)

        return notifications

    def insert_notifications(self, notifications):
        if not notifications:
            return

        # generate placeholders for insert sql statement
        columns = notifications[0].keys()
        insert_sql_stm = INSERT_NOTIFICATIONS_SQL % ', '.join(columns)
        insert_sql_stm += ', '.join(
            ['(%s)' % ', '.join(['%s'] * len(columns))] * len(notifications)
        )
        insert_values = []

        for notification in notifications:
            insert_values.extend([ notification[column] for column in columns ])

        cursor = self.db_conn.cursor()
        cursor.execute(insert_sql_stm, tuple(insert_values))
        cursor.close()

def main():
    """Main method to be run only when module runs as a script."""
    notification_handler = NotificationHandler()
    notification_handler.process_notifications()

if __name__ == '__main__':
    main()