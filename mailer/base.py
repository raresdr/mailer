import logging
import pymysql
import json
import os

class Base:
    """Base class for handling campaign scheduling, triggering and email sending."""

    def __init__(self, log_prefix=''):
        """Method for initializing the scheduler object with conf, logger, db connection handler & triggered counter."""
        # initialize config based on environment
        env = os.getenv('MAILER_ENVIRONMENT')
        with open('%s/../conf/%s.json' %
            (os.path.dirname(os.path.abspath(__file__)), env if env else 'development')
        ) as conf_file:
            self.conf = json.load(conf_file)

        # initialize logger
        log_conf = self.conf['logger']
        logger_name = self.__class__.__name__.lower()
        logger_filename = '%s%s%s' % (log_conf['base_path'], log_prefix, log_conf[logger_name + '_filename'])
        logger_handler = logging.FileHandler(logger_filename)
        logger_handler.setFormatter(logging.Formatter(fmt=log_conf['format'], datefmt=log_conf['datefmt']))
        self.log = logging.getLogger(logger_name)
        self.log.addHandler(logger_handler)
        self.log.setLevel(getattr(logging, log_conf['level']))
        self.log.propagate = False

        # initialize db connection
        db_conf = self.conf['db_info']
        self.db_conn = pymysql.connect(
            host=db_conf['host'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database'],
            autocommit=db_conf['autocommit'],
            use_unicode=db_conf['use_unicode'],
            cursorclass=pymysql.cursors.DictCursor
        )