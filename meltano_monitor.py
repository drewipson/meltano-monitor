#!/usr/local/bin/python
"""
    Usage: Executes after meltano elt operations via the sync.sh script. Queries Meltano System database using environment credentials to send status reports to the Meltano Monitor Slack channel inside Halosight slack group.

    Author: Drew Ipson - 02.01.2022
"""
import psycopg2 as pg, os, requests, logging
from datetime import datetime
from string import Template

# DATETIME Variable established in UTC
UTC_DATETIME = datetime.now().strftime("%B %-m, %Y @ %H:00 UTC")

# DAILY REPORT BOOL
DAILY_SUMMARY = True if datetime.now().hour == 16 else False

# SQL QUERIES
DAILY_REPORT_QUERY = "with job_data as (select case when job_id like '%clickup%' then 'clickup-to-snowflake-prod' else job_id end as \"job_id\",case when state = 'SUCCESS' then 1 else 0 end as \"success_rate\", ended_at-started_at as \"run_time\" from public.job where started_at >= NOW() - interval '24 hours' and state != 'RUNNING') select job_id, avg(success_rate), avg(run_time) from job_data where job_id like '%prod%' group by job_id;"
JOB_FAIL_QUERY = "select case when job_id like '%clickup%' then 'clickup-to-snowflake-prod' else job_id end, state from public.job where started_at >= date_trunc('hour', NOW()) and state = 'FAIL' and job_id like '%prod%'"

# SLACK BLOCK KIT OBJECT
# formats reprot with appropriate styling and layout that will be passed as json data object to slack api 
HOURLY_REPORT_TITLE = ":alert: Meltano Monitor - Hourly Job Report :alert:"
DAILY_SUMMARY_TITLE = ":eyes:  Meltano Monitor - 24 Hour Job Summary :eyes:"
HOURLY_REPORT_TEXT = "This hourly report shows which jobs have failed within the past hour."
DAILY_SUMMARY_TEXT = "This Daily Summary Report shows job performance over the last 24 hours including average run time, average uptime percentage, and a `fail` or `pass` status if uptime percentage is over 95%."
data = {
	"blocks": [
		{
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"{DAILY_SUMMARY_TITLE if DAILY_SUMMARY else HOURLY_REPORT_TITLE}"
			}
		},
		{
			"type": "context",
			"elements": [
				{
					"text": f"*{UTC_DATETIME}*  |  Meltano Job Status",
					"type": "mrkdwn"
				}
			]
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " :snowflake: *SNOWFLAKE DATA LOADS* :snowflake:"
			}
		},
        {
	        "type": "section",
            "text": {
		        "type": "mrkdwn",
		        "text": DAILY_SUMMARY_TEXT if DAILY_SUMMARY else HOURLY_REPORT_TEXT
		    }
	    },
		{
			"type": "divider"
		}
	]
}

# SLACK EMOJI LOOKUP
# enables quick reference to job id and appropriate slack emoji as well as job status emoji.
emoji = {
    "jobs" : {
        "salesforce-to-snowflake-prod": ":salesforce:",
        "meltano-to-snowflake-prod": ":postgresql:",
        "gitlab-to-snowflake-prod": ":gitlab:",
        "tap-sheet-feature-usage-to-snowflake-prod": ":google_spreadsheets:",
        "clickup-to-snowflake-prod": ":clickup:",
        "metadata-to-snowflake-prod": ":postgresql:",
        "daily-aws-cost-to-snowflake-prod": ":aws:",
        "hourly-aws-cost-to-snowflake-prod": ":aws:"
    },
    "status": {
        "SUCCESS": ":pass:",
        "FAIL": ":fail:"
    }
}

# TIMEDELTA STRING FORMATTER
class TimeDeltaTemplate(Template):
    delimiter = "%"

def strftdelta(timedelta: object, format: str) -> str:
    """
    Formats timedelta objects like strftime for datetime objects. Only supports %D, %H, %M, %S.
    """
    d = {"D": timedelta.days}
    d["H"], rem = divmod(timedelta.seconds, 3600)
    d["M"], d["S"] = divmod(rem, 60)
    t = TimeDeltaTemplate(format)
    return t.substitute(**d)

# SLACK BLOCK KIT FORMATTER
# function to append new objects with right formatting to data object
def format_slack_report(row: tuple):
    """
    Adds section dictionary objects to block list set for slack report.
    """
    if DAILY_SUMMARY:
        UPTIME_PERCENTAGE = round(row[1]*100,2)
        data['blocks'].append(
            {
	            "type": "section",
	            "text": {
	                "type": "mrkdwn",
	                "text": f"{emoji['jobs'][row[0]]} *{row[0]}* - {emoji['status']['SUCCESS' if UPTIME_PERCENTAGE > 95.0 else 'FAIL']}"
	            }
	        }
        )
        data['blocks'].append(
            {
	            "type": "context",
	            "elements": [
	                {
	                    "text": strftdelta(row[2],"Avg. Run Time: %M min %S sec"),
	                    "type": "mrkdwn"
	                },
                    {
                        "text": f"Uptime Percentage: {UPTIME_PERCENTAGE}%",
                        "type": "mrkdwn"
                    }
	            ]
	        }
        )
    else:
        data['blocks'].append(
            {
	            "type": "section",
	            "text": {
	                "type": "mrkdwn",
	                "text": f"{emoji['jobs'][row[0]]} *{row[0]}* - {emoji['status'][row[1]]}"
	            }
	        }
        )

# MAIN FUNCTION
def main():
    # attempt database connection
    try:
        conn = pg.connect(os.environ['MELTANO_DATABASE_URI'])
        cur = conn.cursor()
        # execute sql query for report data
        cur.execute(JOB_FAIL_QUERY if not DAILY_SUMMARY else DAILY_REPORT_QUERY)
        results = cur.fetchall()
        if len(results) > 0:
            for row in results:
                format_slack_report(row)
            try:
                requests.post(f"{os.environ['SLACK_WEBHOOK_API']}", json=data)
            except Exception as e:
                logging.error("Error: Could not post to Slack Web Hook API.", exc_info=e)
    # log errors if database connection or query is not successful.
    except Exception as e:
        logging.error("Error: Could not connect to database.", exc_info=e)
        

if __name__ == '__main__':
    main()