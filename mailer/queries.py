'''Module for storing SQL queries and statements across the entire app'''

# Scheduler
SELECT_TRIGGER_CAMPAIGNS_SQL = '''SELECT *
    FROM newsletter
    WHERE status = 'pending'
        AND scheduled_date <= CURRENT_TIMESTAMP'''

UPDATE_TRIGGER_CAMPAIGNS_SQL = '''UPDATE newsletter
    SET status = %s
    WHERE id IN ( %s )'''

UPDATE_END_TRIGGER_CAMPAIGNS_SQL = '''UPDATE newsletter
    SET status = %s, sent_date = CURRENT_TIMESTAMP
    WHERE id IN ( %s )'''

# Campaign
SELECT_CAMPAIGN_SQL = '''SELECT newsletter.*, credential.email as campaign_creator_email
    FROM newsletter
    JOIN user on newsletter.user_id = user.id
    JOIN credential on credential.id = user.credential_id
    WHERE newsletter.id = %s'''

CONSENT_CONDITION = {
    'GDPR': 'unspecified',
    'normal': 'accepted'
}

SELECT_END_USERS_FILTERS = {
    'domain': 'AND appointment.service_id in (%s)',
    'location': 'AND appointment.location_id in (%s)',
    'group': 'AND end_user_has_group.group_id in (%s)',
    'end_user_added_date': 'AND end_user.date_created >= %s',
    'birthday': '''AND DATE_ADD(str_to_date(end_user.birthday, '%%Y-%%m-%%d'), INTERVAL (YEAR(NOW()) - YEAR(str_to_date(end_user.birthday, '%%Y-%%m-%%d'))) YEAR) BETWEEN
		DATE_ADD(str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s'), INTERVAL (YEAR(NOW()) - YEAR(str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s'))) YEAR)
		AND DATE_ADD(str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s'), INTERVAL (YEAR(NOW()) - YEAR(str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s'))) YEAR)''',
    'with_appointments_start_date': 'HAVING max_visit >= %s',
    'without_appointments_start_date': 'HAVING max_visit < %s',
    'exclude': '%s IS NOT NULL'
}

SELECT_END_USERS_VAR_COLS = {
    'last_name': 'end_user.last_name',
    'first_name': 'end_user.first_name',
    'birthday': 'DATE_FORMAT(end_user.birthday, "%%d/%%m/%%Y") birthday',
    'group_name': 'end_user_group.name AS group_name',
    'unsubscribe_code': 'end_user.code AS unsubscribe_code',
    'age': '''CASE
                  WHEN end_user.birthday IS NOT NULL THEN TIMESTAMPDIFF(YEAR, end_user.birthday, CURDATE())
                  ELSE NULL
              END age''',
    'first_visit': '''MIN(
    	                  CASE
			                  WHEN appointment.local_time <= CURRENT_DATE AND appointment_status.name in ('showedup', 'paid', 'confirmed') THEN DATE_FORMAT(appointment.local_time, "%%d/%%m/%%Y")
                              ELSE NULL
		                  END
	                  ) as first_visit''',
    'last_visit': '''MAX(
		                 CASE
			                 WHEN appointment.local_time <= CURRENT_DATE AND appointment_status.name in ('showedup', 'paid', 'confirmed') THEN DATE_FORMAT(appointment.local_time, "%%d/%%m/%%Y")
                             ELSE NULL
		                 END
	                 ) as last_visit''',
    'next_visit': '''MIN(
    		             CASE
	    		             WHEN appointment.local_time > CURRENT_DATE AND appointment_status.name in ('showedup', 'paid', 'confirmed') THEN DATE_FORMAT(appointment.local_time, "%%d/%%m/%%Y")
                             ELSE NULL
		                 END
	                 ) as next_visit''',
}

END_USERS_SQL_SELECT = '''SELECT
        end_user.id,
        end_user.email,
        end_user.consent_status,
	    MAX(
            CASE
                WHEN appointment_status.name in ('showedup', 'paid', 'confirmed') THEN appointment.local_time
                ELSE NULL
            END
        ) as max_visit,
        %s'''
END_USERS_SQL_QUERY = '''FROM end_user
        LEFT JOIN end_user_has_group ON end_user_has_group.user_id = end_user.id
        LEFT JOIN end_user_group ON end_user_group.group_id = end_user_has_group.group_id
        LEFT JOIN appointment_has_end_user ON appointment_has_end_user.end_user_id = end_user.id
        LEFT JOIN appointment ON appointment.id = appointment_has_end_user.appointment_id
        LEFT JOIN appointment_has_status ON appointment_has_end_user.appointment_id = appointment_has_status.appointment_id
        LEFT JOIN appointment_status ON appointment_has_status.status_id = appointment_status.id
    WHERE end_user.deleted = 0
        AND end_user.email IS NOT NULL
        AND end_user.black_tag = 0
        AND end_user.business_id = %s
        AND end_user.consent_status = %s'''

# Stats Collector
SELECT_STATS_CAMPAIGNS_SQL = '''SELECT id
    FROM newsletter
    WHERE scheduled_date BETWEEN %s AND %s
        AND status in ('sending', 'closed')'''

UPDATE_STATS_CAMPAIGNS_SQL = '''UPDATE newsletter
    SET stats = %s
    WHERE id = %s'''

# SNS Notifications
INSERT_NOTIFICATIONS_SQL = 'INSERT INTO newsletter_email_notifications(%s) VALUES '