from queries import SELECT_CAMPAIGN_SQL, END_USERS_SQL_SELECT, END_USERS_SQL_QUERY, CONSENT_CONDITION, SELECT_END_USERS_FILTERS, SELECT_END_USERS_VAR_COLS
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import concurrent.futures as cf
from copy import deepcopy
from base import Base
import html2text
import boto3
import time
import json
import os

class Campaign(Base):
    """Class for processing a specific campaign."""

    def __init__(self, id):
        """Using parent's init method for initializing the campaign object with conf, logger, db connection handler."""
        super().__init__(log_prefix=id)

        # retrieve the associated campaign
        cursor = self.db_conn.cursor()
        cursor.execute(SELECT_CAMPAIGN_SQL, (id))
        self.campaign_entry = cursor.fetchone()
        cursor.close()
        if not self.campaign_entry:
            raise Exception('Campaign id %d entry could not be retrieved.' % id)
        if not self.campaign_entry['template_path']:
            raise Exception('No template file path for campaign id %d found.' % id)

        # initialize template content and MIME object
        try:
            with open(self.campaign_entry['template_path'], 'r') as template_file:
                self.template_content = template_file.read()
            if not self.template_content:
                raise Exception('No template content retrieved from file path for campaign id %d.' % id)

            self.msg_obj = self.initialize_mime_object()
        except Exception as e:
            self.log.exception(e)
            raise Exception('Campaign id %d template retrieval or MIME object initialization failed. Check campaign log for more details' % self.campaign_entry['id'])

        self.end_users = []

        self.ses = boto3.client(
            'ses',
            region_name=self.conf['aws']['region'],
            aws_access_key_id=self.conf['aws']['aws_access_key_id'],
            aws_secret_access_key=self.conf['aws']['aws_secret_access_key']
        )

    def initialize_mime_object(self):
        """Method for intializing MIME object used for generating the email content"""
        msg_obj = MIMEMultipart('mixed')
        msg_obj['Subject'] = self.campaign_entry['subject']
        msg_obj['From'] = self.conf['aws']['ses']['from_email']
        msg_obj.add_header('CAMPAIGN_ID', str(self.campaign_entry['id']))

        # add image source to template if there's any
        if self.campaign_entry['image_path']:
            self.template_content = self.template_content.replace('{{image_source}}', self.conf['image_base_url'] + self.campaign_entry['image_path'])

        return msg_obj

    def process(self):
        """Method for processing an active campaign."""
        self.log.info('Started processing campaign with id %d...' % self.campaign_entry['id'])

        try:
            self.end_users = self.retrieve_end_users()
            self.log.info('%d end users who qualify for the campaign found.' % ( len(self.end_users) if self.end_users else 0))
            if self.end_users:
                # on development or staging environment, send the email to only one recipient and use the campaign creator's email
                if os.getenv('MAILER_ENVIRONMENT') != 'production':
                    self.end_users[0]['email'] = self.campaign_entry['campaign_creator_email']
                    self.end_users = self.end_users[:1]
                start = time.time()
                # use threads to improve overall duration of the external SES api requests processing
                with cf.ThreadPoolExecutor(self.conf['thread_workers_no']) as ex:
                    res = ex.map(self.send_email, self.end_users[:200])

                response_list = list(res)
                self.log.info('Sending %d emails took %f.' % (len(response_list), time.time() - start))
        except Exception as e:
            self.log.exception(e)
            raise Exception('Campaign id %d processing failed. Check campaign log for more details' % self.campaign_entry['id'])
        else:
            self.log.info('Ended processing campaign with id %d.' % self.campaign_entry['id'])

    def retrieve_end_users(self):
        """Method for retrieving end users who qualify for the campaign."""
        sql_query, sql_params = self.eu_filters_sql_and_params()

        cursor = self.db_conn.cursor()
        #self.log.info(cursor.mogrify(sql_query, sql_params))
        cursor.execute(sql_query, sql_params)
        end_users = cursor.fetchall()
        cursor.close()

        return end_users

    def eu_filters_sql_and_params(self):
        # first concatenate select columns which might be used as replaceable variables to the query
        sql_query = END_USERS_SQL_SELECT % ', '.join(list(SELECT_END_USERS_VAR_COLS.values())) + ' ' + END_USERS_SQL_QUERY
        sql_end_part_query = 'GROUP BY end_user.id'

        # add bind params & sql query conditions
        query_params = [self.campaign_entry['business_id'], CONSENT_CONDITION[self.campaign_entry['type']]]
        query_params_end = []
        filters = json.loads(self.campaign_entry['filters'])
        for filter in filters:
            if filter not in SELECT_END_USERS_FILTERS:
                raise Exception('Unknown campaign filter %s retrieved from db' % filter)
            elif not filters[filter]:
                continue

            if filter == 'domain':
                if not isinstance(filters[filter], list):
                    raise Exception('Invalid filter format for %s' % filter)
                service_ids = []
                for domain in filters[filter]:
                    if not isinstance(domain, dict) or 'service' not in domain or not isinstance(domain['service'], list) or not len(domain['service']):
                        raise Exception('Invalid filter format for %s - service' % filter)
                    service_ids.extend(domain['service'])
                if len(service_ids):
                    query_params.extend(service_ids)
                    sql_condition = SELECT_END_USERS_FILTERS[filter] % ', '.join(['%s'] * len(service_ids))
                    sql_query += ' ' + sql_condition
            elif filter in ('group', 'location'):
                if not isinstance(filters[filter], list):
                    raise Exception('Invalid filter format for %s' % filter)
                query_params.extend(filters[filter])
                sql_condition = SELECT_END_USERS_FILTERS[filter] % ', '.join(['%s'] * len(filters[filter]))
                sql_query += ' ' + sql_condition
            elif filter == 'birthday':
                if not isinstance(filters[filter], list) or len(filters[filter]) != 2:
                    raise Exception('Invalid filter format for %s' % filter)
                query_params.extend([filters[filter][0], filters[filter][0], filters[filter][1], filters[filter][1]])
                sql_query += ' ' + SELECT_END_USERS_FILTERS[filter]
            elif filter in ('with_appointments_start_date', 'without_appointments_start_date'):
                if not isinstance(filters[filter], str):
                    raise Exception('Invalid filter format for %s' % filter)
                query_params_end.append(filters[filter])
                sql_end_part_query += ' ' + SELECT_END_USERS_FILTERS[filter]
            elif filter == 'exclude':
                if not isinstance(filters[filter], list):
                    raise Exception('Invalid filter format for %s' % filter)
                # no params needed here, only add sql conditions
                [ column_name for column_name in filters[filter]  ]
                sql_condition = SELECT_END_USERS_FILTERS[filter] % ', '.join(['%s'] * len(filters[filter]))
                sql_query += ' ' + sql_condition
            else:
                # for now this executes only for end_user_added_date filter
                if not isinstance(filters[filter], str):
                    raise Exception('Invalid filter format for %s' % filter)
                query_params.append(filters[filter])
                sql_query += ' ' + SELECT_END_USERS_FILTERS[filter]

        sql_query += ' ' + sql_end_part_query
        query_params.extend(query_params_end)

        return (sql_query, tuple(query_params))

    def send_email(self, end_user):
        """Method for sending an email by calling AWS SES."""
        # on development or staging environment, send the email to the campaign creator
        # test_emails = ['bounce@simulator.amazonses.com', 'complaint@simulator.amazonses.com']
        to_email = end_user['email']
        # replace template variables
        html_body = self.template_content
        for var in SELECT_END_USERS_VAR_COLS:
            html_body = html_body.replace(
                '{{%s}}' % var,
                str(end_user[var]) if end_user[var] else ''
            )
        text_body = html2text.html2text(html_body)

        # main MIME object
        msg_obj = deepcopy(self.msg_obj)
        msg_obj.add_header('END_USER_ID', str(end_user['id']))

        # encapsulate the plain and HTML versions of the message body in an
        # 'alternative' part, so message agents can decide which they want to display
        msg_alternative = MIMEMultipart('alternative')
        msg_obj.attach(msg_alternative)

        msg_alternative.attach(MIMEText(html_body, 'html', 'UTF-8'))
        msg_alternative.attach(MIMEText(text_body, 'plain', 'UTF-8'))

        response = self.ses.send_raw_email(
            Source=self.conf['aws']['ses']['from_email'],
            Destinations=[to_email],
            Tags=[
                {'Name': self.conf['aws']['ses']['message_tag'], 'Value': str(self.campaign_entry['id'])}
            ],
            ConfigurationSetName=self.conf['aws']['ses']['configuration_set'],
            RawMessage={
                'Data': msg_obj.as_string()
            }
        )

        return response