from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import random

@dag(start_date=days_ago(1), schedule_interval="@daily", tags=["example", "branching"])
def taskflow_branching_dag():
    @task
    def extract_data():
        """Simulate data extraction."""
        return [random.randint(1, 10) for _ in range(5)]

    @task.branch
    def decide_which_path(data):
        """Decide which path(s) to take based on data content."""
        total = sum(data)
        print(f"Total sum of data: {total}")

        if total > 25:
            return "process_high_volume"
        elif 15 < total <= 25:
            return ["process_medium_volume", "notify_medium_volume"]
        else:
            return "process_low_volume"

    @task
    def process_high_volume():
        """Process higher volume data."""
        print("Processing high volume of data.")

    @task
    def process_medium_volume():
        """Process medium volume data."""
        print("Processing medium volume of data.")

    @task
    def process_low_volume():
        """Process low volume of data."""
        print("Processing low volume of data.")

    @task
    def notify_medium_volume():
        """Send notification for medium volume."""
        print("Notification sent for medium data volume processing.")

    @task(trigger_rule="none_failed")
    def final_task():
        """Final task that should run after the selected path."""
        print("Final task executed.")

    # Define tasks
    data = extract_data()
    branch_task = decide_which_path(data)

    process_high = process_high_volume()
    process_medium = process_medium_volume()
    process_low = process_low_volume()
    notify_medium = notify_medium_volume()
    final = final_task()

    # Set proper branching dependencies
    branch_task >> [process_high, process_medium, process_low, notify_medium]

    # Ensure skipped tasks don't break execution
    [process_high, process_medium, process_low, notify_medium] >> final

workflow = taskflow_branching_dag()
