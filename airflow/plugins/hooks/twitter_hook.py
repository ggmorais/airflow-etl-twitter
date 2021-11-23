from airflow.providers.http.hooks.http import HttpHook
import requests
import json


class TwitterHook(HttpHook):
    def __init__(self, query, conn_id=None, start_time=None, end_time=None):
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time

        super().__init__(http_conn_id=self.conn_id)

    def connect_to_endpoint(self, session, url, params):
        response = requests.Request("GET", url=url, params=params)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {response.url}")
        return self.run_and_check(session, prep, {}).json()

    def create_query(self):
        query_params = {
            'query': self.query,
            'tweet.fields': 'author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text',
            'expansions': 'author_id', 
            'user.fields': 'id,name,username,created_at',
            'start_time': self.start_time,
            'end_time': self.end_time,
            'max_results': 100
        }

        self.log.info(query_params)

        return query_params

    def run(self):
        session = self.get_conn()

        return self.connect_to_endpoint(session, f"{self.base_url}/2/tweets/search/recent", self.create_query())

if __name__ == "__main__":
    tw = TwitterHook("AluraOnline")
    data = tw.run()

    print(json.dumps(data, indent=2, sort_keys=True))
