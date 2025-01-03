from airflow.operators.slack_operator import SlackAPIPostOperator

def task_fail_slack_alert(context):
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        slack_conn_id = 'slack_cpe',
        channel='#airflow-notifications',
        text=':red_circle: Task Failed - data will not be updated soon',
    )
    return failed_alert.execute()