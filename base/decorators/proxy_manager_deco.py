
from base.tasks.helpers.config_data import InitialData
import requests


def use_proxy(func):
    def wrap(self):
        proxy_manager_addr = InitialData.proxy_manager_addr
        data = {
            "api_key": self.api_key,
            "body": self.body,
            "url": self.url,
            "request_type": self.request_type,
            "header": self.header,
        }

        try:
            if 'v1' in self.body or 'v1 in self.url':
                print('---getting a proxy for v1---')
                proxy = requests.post(proxy_manager_addr, data={'request_type': self.request_type, 'api_version': 'v1'})
            else:
                print('---getting a proxy for v2/v3 or ozon---')
                proxy = requests.post(proxy_manager_addr, data={'request_type': self.request_type, 'api_version': 'v2'})

            proxy_answer_json = proxy.json()
            code = proxy_answer_json['code']
            text = proxy_answer_json['text']

            print(proxy_answer_json)

            if proxy_answer_json['code'] in [500, 429]:
                print(f'---response object code is {code}')
                raise Exception
        except Exception as e:
            print('---proxy manager is not responding properly, no proxy instead---')
            print(e)
            response_object = func(self)
            return response_object[0], response_object[1]

        return text, code

    return wrap


