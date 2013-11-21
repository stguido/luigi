import configuration
import logging
import task_history

logger = logging.getLogger("luigi.task_history")

class LogTaskHistory(task_history.TaskHistory):
  ''' Log when a task is scheduled, started and task_finished'''

  def task_scheduled(self, task_id):
    logger.info("Task %s scheduled", task_id)

  def task_finished(self, task_id, successful):
    if successful:
      logger.info("Task %s done", task_id)
    else:
      logger.info("Task %s failed", task_id)

  def task_started(self, task_id, worker_host):
    logger.info("Task %s started on %s", task_id, worker_host)


# check if we need to setup a specific handler for the task history logger
config = configuration.get_config()
log_path = config.get('scheduler', 'log_task_history_path', None)
if log_path:
  ch = logging.FileHandler(filename=log_path)
  ch.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s - %(message)s')
  ch.setFormatter(formatter)
  logger.addHandler(ch)
