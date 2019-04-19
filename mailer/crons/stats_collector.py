import os, sys
# workaround for a parent directory import to not fail
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import json
import boto3
import datetime
from base import Base
from queries import SELECT_STATS_CAMPAIGNS_SQL, UPDATE_STATS_CAMPAIGNS_SQL

class StatsCollector(Base):
    """Class for handling cloudwatch statistics."""

    def __init__(self):
        """Using parent's init method for initializing the scheduler object with conf, logger, db connection handler."""
        super().__init__()

        # initialize stats attribute
        self.stats = {}

        # initialize aws client
        self.cloudwatch = boto3.client(
            'cloudwatch',
            region_name=self.conf['aws']['region'],
            aws_access_key_id=self.conf['aws']['aws_access_key_id'],
            aws_secret_access_key=self.conf['aws']['aws_secret_access_key']
        )

    def get_stats(self, campaign_ids=()):
        """Method for retrieving ses statistics."""
        start_time = (datetime.datetime.utcnow() - datetime.timedelta(days=self.conf['aws']['cloudwatch']['start_time_day_offset']))\
            .replace(hour=0, minute=0, second=0)
        end_time = (datetime.datetime.utcnow() - datetime.timedelta(days=self.conf['aws']['cloudwatch']['end_time_day_offset']))
        start_time_formatted = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time_formatted = end_time.strftime('%Y-%m-%d %H:%M:%S')

        self.log.info('Started retrieving ses statistics for %s to %s...' % (start_time_formatted, end_time_formatted))

        try:
            if not len(campaign_ids):
                cursor = self.db_conn.cursor()
                cursor.execute(SELECT_STATS_CAMPAIGNS_SQL, (start_time_formatted, end_time_formatted))
                campaign_ids = [campaign_entry['id'] for campaign_entry in cursor.fetchall()]
                cursor.close()

            for campaign_id in campaign_ids:
                # build metric data queries list
                metric_data_queries = [
                    {
                        'Id': metric['id'],
                        'MetricStat': {
                            'Metric': {
                                'Namespace': metric['namespace'],
                                'MetricName': metric['metric_name'],
                                'Dimensions': [
                                    {
                                        'Name': metric['dimension_name'],
                                        'Value': str(campaign_id)
                                    },
                                ]
                            },
                            'Period': metric['period'],
                            'Stat': metric['stat'],
                            'Unit': metric['unit']
                        }
                    } for metric in self.conf['aws']['cloudwatch']['metrics']
                ]

                response = self.cloudwatch.get_metric_data(
                    MetricDataQueries=metric_data_queries,
                    StartTime=start_time.timestamp(),
                    EndTime=end_time.timestamp(),
                )

                if response and 'MetricDataResults' in response:
                    self.stats[campaign_id] = {}
                    for metric in response['MetricDataResults']:
                        self.stats[campaign_id][metric['Id']] = sum(metric['Values'])
        except Exception as e:
            self.log.exception(e)

        self.log.info('Ended retrieving ses statistics...')

    def update_campaigns(self):
        cursor = self.db_conn.cursor()
        for campaign_id in self.stats:
            campaign_stats = self.stats[campaign_id]
            cursor.execute(UPDATE_STATS_CAMPAIGNS_SQL, (json.dumps(campaign_stats), campaign_id))
        cursor.close()

def main():
    """Main method to be run only when module runs as a script."""
    stats_collector = StatsCollector()
    stats_collector.get_stats()
    stats_collector.update_campaigns()

if __name__ == '__main__':
    main()