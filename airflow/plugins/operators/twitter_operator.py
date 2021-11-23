from airflow.models import DAG, BaseOperator, TaskInstance
from hooks.twitter_hook import TwitterHook

import json
import time
import pathlib
import datetime as dt


class TwitterOperator(BaseOperator):
    template_fields = ('query', 'start_time', 'end_time', 'file_path')

    def __init__(self, query, file_path, conn_id=None, start_time=None, end_time=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context=None):
        hook = TwitterHook(self.query, self.conn_id, self.start_time, self.end_time)
        data = hook.run()

        self.log.info(self.file_path)
        
        pathlib.Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.file_path, "w") as f:
            json.dump(data, f, ensure_ascii=False)
