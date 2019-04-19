import os, sys
# workaround for a parent directory import to not fail
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from base import Base
from campaign import Campaign
from queries import SELECT_TRIGGER_CAMPAIGNS_SQL, UPDATE_TRIGGER_CAMPAIGNS_SQL, UPDATE_END_TRIGGER_CAMPAIGNS_SQL

class Scheduler(Base):
    """Class for handling campaign scheduling."""

    def __init__(self):
        """Using parent's init method for initializing the scheduler object with conf, logger, db connection handler."""
        super().__init__()

        # initialize triggered campaigns counter
        self.triggered = 0

    def trigger_campaigns(self):
        """Method for triggering pending campaigns."""
        self.log.info('Started active campaigns to be triggered check...')

        # prepare a db cursor object
        # and retrieve all active campaigns to be triggered
        cursor = self.db_conn.cursor()
        cursor.execute(SELECT_TRIGGER_CAMPAIGNS_SQL)
        campaigns = cursor.fetchall()

        if not campaigns:
            self.log.info('No active campaigns found.')
            return

        # update status for all campaigns to be triggered as in progress(sending)
        campaign_ids = ', '.join([ str(campaign_entry['id']) for campaign_entry in campaigns ])
        cursor.execute(UPDATE_TRIGGER_CAMPAIGNS_SQL, ('sending', campaign_ids))

        for campaign_entry in campaigns:
            try:
                self.log.info('Started processing campaign with id: %d, name: %s, business_id: %d...' % (campaign_entry['id'], campaign_entry['name'], campaign_entry['parent_id']))
                campaign = Campaign(campaign_entry['id'])
                campaign.process()
            except Exception as e:
                self.log.exception(e)
                # update campaign's status as error
                cursor.execute(UPDATE_TRIGGER_CAMPAIGNS_SQL, ('error', campaign_entry['id']))
            else:
                self.triggered += 1
                # update campaign's status as closed
                cursor.execute(UPDATE_END_TRIGGER_CAMPAIGNS_SQL, ('closed', campaign_entry['id']))

        cursor.close()
        self.log.info('Ended active campaigns to be triggered check. Successfully triggered %d out of %d campaigns.' % (self.triggered, len(campaigns)))

def main():
    """Main method to be run only when module runs as a script."""
    scheduler = Scheduler()
    scheduler.trigger_campaigns()

if __name__ == '__main__':
    main()