"""
A Secretaria de Saúde do Paraná liberou um tipo de boletim ao longo dos
primeiros e dias e outro tipo, mais recentemente:

- O primeiro possui informações específicas sobre cada paciente (se viajou, de
  onde etc.);
- O segundo possui informações gerais sobre casos por município.

Esse script coleta o segundo tipo de boletim.
"""

import os
import re
from urllib.parse import urljoin
from pathlib import Path

import scrapy
import rows
from rows.plugins.plugin_pdf import PyMuPDFBackend, same_column

from utils import CleanIntegerField


BASE_PATH = Path(__file__).parent
DOWNLOAD_PATH = BASE_PATH / "data" / "download"

MONTHS = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]

RE_DAY = "(?P<day>\d{1,2})"
RE_MONTH = "(?P<month>\d{1,2})"
RE_MONTH_EXT = "(?P<month_ext>[a-zà-ü]+)"
RE_YEAR = "(?P<year>\d{2,4})"

RE_DATE_1 = re.compile(f"Distrito Federal,\s{RE_DAY}/{RE_MONTH}/{RE_YEAR}")
RE_DATE_2 = re.compile(f"Distrito Federal,\s+dia\s+{RE_DAY}\s+de\s+{RE_MONTH_EXT},")

RE_DATES = [RE_DATE_1, RE_DATE_2]

RE_DIGIT = "([-]|\d+)"
RE_DIGIT_TOTAL = f"(?P<total>{RE_DIGIT})"
RE_DIGIT_INVESTIGATION = f"(?P<investigation>{RE_DIGIT})"
RE_DIGIT_CONFIRMED = f"(?P<confirmed>{RE_DIGIT})"
RE_DIGIT_DISCARDED = f"(?P<discarded>{RE_DIGIT})"

RE_CASES_1 = re.compile(f"Em\s+investigação\s+Confirmado[s]?\s+Descartado[s]?\s+{RE_DIGIT_INVESTIGATION}\s+{RE_DIGIT_CONFIRMED}\*?\s+{RE_DIGIT_DISCARDED}\s+{RE_DIGIT_TOTAL}\s")
RE_CASES_2 = re.compile(f"Excluído[s]?\d?\s+Caso[s]?\s+suspeito[s]?\d?\s+Total\s*Confirmado[s]?\d?\s+Em\s+investigação\d?\s+Descartado[s]?\d?\s+{RE_DIGIT}\s+{RE_DIGIT_CONFIRMED}\s+{RE_DIGIT_INVESTIGATION}\s+{RE_DIGIT_DISCARDED}\s+{RE_DIGIT}")
RE_CASES_3 = re.compile(f"Caso[s]?\s+Notificado[s]?\s+Total\s+(Suspeito[s]?|Em\s+investigação)\s+Confirmado[s]?\s+Descartado[s]?\s+{RE_DIGIT_INVESTIGATION}\s+{RE_DIGIT_CONFIRMED}\*?\s+{RE_DIGIT_DISCARDED}\s+{RE_DIGIT_TOTAL}")

RE_CASES = [RE_CASES_1,
            RE_CASES_2,
            RE_CASES_3]

def parse_pdf(filename, meta):
    # Extract update date
    pdf_doc = PyMuPDFBackend(filename)
    update_date = None
    for page in pdf_doc.objects():
        for obj in page:
            if REGEXP_UPDATE.match(obj.text):
                update_date = PtBrDateField.deserialize(REGEXP_UPDATE.findall(obj.text)[0])
                break
    if update_date is None:  # String not found in PDF
        # Parse URL to get date inside PDF's filename
        date = meta["boletim_url"].split("/")[-1].split(".pdf")[0].replace("CORONA_", "").split("_")[0]
        update_date = PtBrDateField2.deserialize(date)

    # Extract rows and inject update date and metadata
    table = rows.import_from_pdf(filename, backend="min-x0")
    for row in table:
        if row.municipio == "TOTAL GERAL":
            continue
        row = row._asdict()
        row["data"] = update_date
        row.update(meta)
        yield convert_row(row)


class CoronaDFSpider(scrapy.Spider):
    name = "corona-df"
    start_urls = ["http://www.saude.df.gov.br/informativos-do-centro-de-operacoes-de-emergencia-coe"]

    def parse(self, response):

        for link in response.xpath("//a[contains(@href, '.pdf')]"):
            data = {
                "boletim_titulo": link.xpath(".//text()").extract_first(),
                "boletim_url": urljoin(response.url, link.xpath(".//@href").extract_first()),
            }
            if not "informe" in data["boletim_titulo"].lower():
                continue

            yield scrapy.Request(
                url=data["boletim_url"],
                meta={"row": data},
                callback=self.parse_pdf,
            )

    def parse_pdf(self, response):
        filename = DOWNLOAD_PATH / Path(response.url).name
        with open(filename, mode="wb") as fobj:
            fobj.write(response.body)

        meta = response.meta["row"]
        pdf_doc = rows.plugins.pdf.PyMuPDFBackend(filename)
        pdf_text = "".join(item for item in pdf_doc.extract_text() if item.strip())

        for re_date in RE_DATES:
            date_search = re_date.search(pdf_text, re.IGNORECASE)
            if date_search:
                groups = date_search.groupdict()
                day = groups.get("day")
                if "month" in groups:
                    month = groups.get("month")
                elif "month_ext" in groups:
                    month = groups.get("month_ext")
                    month = MONTHS.index(month) + 1
                if "year" in groups:
                    year = groups.get("year")
                else:
                    year = 2020
                print(f"{day}/{month}/{year}")
                break

        for re_cases in RE_CASES:
            cases_search = re_cases.search(pdf_text, re.IGNORECASE)
            if cases_search:
                groups = cases_search.groupdict()
                total = groups.get("total") if "total" in groups else ""
                investigation = groups.get("investigation") if "investigation" in groups else ""
                confirmed = groups.get("confirmed")
                discarded = groups.get("discarded")
                print(f' - Investigados: {investigation}\n - Confirmados: {confirmed}\n - Descartados: {discarded}')
                break
                
        return {
            "date": f"{year}-{month}-{day}",
            "state": "DF",
            "city": "",
            "place_type": "state",
            "notified": CleanIntegerField.deserialize(total),
            "confirmed": CleanIntegerField.deserialize(confirmed),
            "discarded": CleanIntegerField.deserialize(discarded),
            "suspect": CleanIntegerField.deserialize(investigation),
            "deaths": 0, 
            "notes": "",
            "source_url": response.url
        }