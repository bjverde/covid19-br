import datetime
import json
from urllib.parse import urlencode, urljoin

import scrapy
from scrapy.http.cookies import CookieJar

import date_utils

STATES = "AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO".split()


def qs_to_dict(data):
    """
    >>> qs_to_dict([("a", 1), ("b", 2)])
    {'a': 1, 'b': 2}
    >>> qs_to_dict([("b", 0), ("a", 1), ("b", 2)])
    {'a': 1, 'b': [0, 2]}
    """
    from collections import defaultdict
    new = defaultdict(list)
    for key, value in data:
        new[key].append(value)
    return {
        key: value if len(value) > 1 else value[0]
        for key, value in new.items()
    }

class DeathsSpider(scrapy.Spider):
    name = "obitos"

    login_url = "https://transparencia.registrocivil.org.br/registral-covid"

    registral_url = (
        "https://transparencia.registrocivil.org.br/api/covid-covid-registral"
    )

    xsrf_token = ""

    causes_map = {
        "sars": "SRAG",
        "pneumonia": "PNEUMONIA",
        "respiratory_failure": "INSUFICIENCIA_RESPIRATORIA",
        "septicemia": "SEPTICEMIA",
        "indeterminate": "INDETERMINADA",
        "others": "OUTRAS",
        "covid19": "COVID",
    }

    cookie_jar = CookieJar()

    def start_requests(self):
        yield self.make_login_request()

    def make_login_request(self):
        return scrapy.Request(
            url=self.login_url,
            callback=self.parse_login_response,
            meta={"dont_cache": True},
        )

    def parse_login_response(self, response):
        self.cookie_jar.extract_cookies(response, response.request)
        self.xsrf_token = next(c for c in self.cookie_jar if c.name == "XSRF-TOKEN").value

        for state in STATES:
            for year in [2020, 2019]:
                yield self.make_registral_request(
                    start_date=datetime.date(year, 1, 1),
                    end_date=datetime.date(year, 12, 31),
                    state=state,
                    dont_cache=True,
                )

    def make_registral_request(
        self, start_date, end_date, state, dont_cache=False
    ):
        data = [
            ("chart", "chart5"),
            ("city_id", "all"),
            ("end_date", str(end_date)),
            ("places[]", "HOSPITAL"),
            ("places[]", "DOMICILIO"),
            ("places[]", "VIA_PUBLICA"),
            ("places[]", "OUTROS"),
            ("start_date", str(start_date)),
            ("state", state),
        ]
        return scrapy.Request(
            url=urljoin(self.registral_url, "?" + urlencode(data)),
            headers={"X-XSRF-TOKEN" : self.xsrf_token},
            callback=self.parse_registral_request,
            meta={"row": qs_to_dict(data), "dont_cache": dont_cache},
        )

    def parse_registral_request(self, response):
        state = response.meta["row"]["state"]
        data = json.loads(response.body)

        for date, chart in data["chart"].items():
            row = {"date": date, "state": state}
            for cause, portuguese_name in self.causes_map.items():
                row[cause] = chart[portuguese_name][0]["total"] if portuguese_name in chart else None
            
            yield row