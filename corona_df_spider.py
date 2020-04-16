"""
A Secretaria de Saúde do Distrito Federal publica boletins informativos sobre
a COVID-19 em formato PDF. Os documentos são majoritariamente em formato de texto
livre e possuem uma tabela com números referentes a casos da unidade da federação
como um todo, sem disntinção de municípios ou regiões administrativas
até o dia 19/03/2020.

Alterações do formato nos dias
 - 20/03/2020
 - 21/03/2020 - Não teve relatorio
 - 22/03/2020 
 - 23/03/2020
 - 24/03/2020 - Não teve relatorio
 - 25/03/2020 - Não teve relatorio
 - 26/03/2020 - inicio dos dados por região, informando apenas os confirmados.
 - 27/03/2020 - 
"""

import re
from urllib.parse import urljoin
from pathlib import Path

import scrapy
import rows

from utils import CleanIntegerField

import datetime

BASE_PATH = Path(__file__).parent
DOWNLOAD_PATH = BASE_PATH / "data" / "download"

MONTHS = ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
          "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]

RE_DAY = "(?P<day>\d{1,2})"
RE_MONTH = "(?P<month>\d{1,2})"
RE_MONTH_EXT = "(?P<month_ext>[a-zà-ü]+)"
RE_YEAR = "(?P<year>\d{2,4})"

RE_DATE_1 = re.compile(f"Distrito Federal,\s{RE_DAY}/{RE_MONTH}/{RE_YEAR}")
RE_DATE_2 = re.compile(f"Distrito Federal,\s+dia\s+{RE_DAY}\s+de\s+{RE_MONTH_EXT},")

RE_DATES = [RE_DATE_1, RE_DATE_2]

RE_FLOAT = "(\d+\,\d+)"
RE_DIGIT = "([-]|\d+)"
RE_DIGIT_TOTAL = f"(?P<total>{RE_DIGIT})"
RE_DIGIT_DEATHS = f"(?P<deaths>{RE_DIGIT})"
RE_DIGIT_INVESTIGATION = f"(?P<investigation>{RE_DIGIT})"
RE_DIGIT_CONFIRMED = f"(?P<confirmed>{RE_DIGIT})"
RE_DIGIT_DISCARDED = f"(?P<discarded>{RE_DIGIT})"

RE_CASES_1 = re.compile(f"Em\s+investigação\s+Confirmado[s]?\s+Descartado[s]?\s+"
                        f"{RE_DIGIT_INVESTIGATION}\s+{RE_DIGIT_CONFIRMED}\*?\s+{RE_DIGIT_DISCARDED}\s+{RE_DIGIT_TOTAL}\s")
RE_CASES_2 = re.compile(f"Excluído[s]?\d?\s+Caso[s]?\s+suspeito[s]?\d?\s+Total\s*Confirmado[s]?\d?\s+Em\s+investigação"
                        f"\d?\s+Descartado[s]?\d?\s+{RE_DIGIT}\s+{RE_DIGIT_CONFIRMED}\s+{RE_DIGIT_INVESTIGATION}\s+{RE_DIGIT_DISCARDED}\s+{RE_DIGIT}")
RE_CASES_3 = re.compile(f"Caso[s]?\s+Notificado[s]?\s+Total\s+(Suspeito[s]?|Em\s+investigação)\s+Confirmado[s]?\s+"
                        f"Descartado[s]?\s+{RE_DIGIT_INVESTIGATION}\s+{RE_DIGIT_CONFIRMED}\*?\s+{RE_DIGIT_DISCARDED}\s+{RE_DIGIT_TOTAL}")

RE_CASES = [RE_CASES_1,
            RE_CASES_2,
            RE_CASES_3]


def getDataMarco01():
    return datetime.datetime(2020, 3, 19)

def getDataMarco20200327():
    return datetime.datetime(2020, 3, 27)

def getDataMarcoAtual():
    return datetime.datetime(2020, 3, 28)

def getNumMes(nomeMes):
    mes=["jan","fev","mar","abr"]
    numMes = mes.index(nomeMes)
    numMes = numMes + 1
    return numMes

def getDataBoletim(texto):
    texto = texto.split('–')
    texto = texto[1]
    texto = texto.strip()
    day = texto[0:2]
    month = getNumMes(texto[2:5])
    year = "20"+texto[5:7]
    date = datetime.datetime(int(year), int(month), int(day))
    return date

def getPdfText(response):
    filename = DOWNLOAD_PATH / Path(response.url).name
    with open(filename, mode="wb") as fobj:
        fobj.write(response.body)

    pdf_doc = rows.plugins.pdf.PyMuPDFBackend(filename)
    pdf_text = "".join(item for item in pdf_doc.extract_text() if item.strip())
    return pdf_text

def buscaTextoTabela20200327(pdf_text):
    RE_CASES_TB1 = re.compile(f"{RE_DIGIT}?\s+"
                              f"Total\s+{RE_DIGIT_TOTAL}\s+{RE_FLOAT}\s+{RE_FLOAT}\s+"
                              f"Fonte: PAINEL COVID-19. Dados atualizados\s+")

    RE_CASES = [RE_CASES_TB1]
    resultado = None
    for re_cases in RE_CASES:
        cases_search = re_cases.search(pdf_text, re.IGNORECASE)
        if cases_search:
            groups = cases_search.groupdict()
            total  = groups.get("total")  if "total" in groups else None
            deaths = groups.get("deaths") if "deaths" in groups else None
            resultado = {
                "notified": total,
                "deaths"  : deaths
            }
            break

    return resultado


def buscaTextoTabela01Geral(pdf_text):
    RE_CASES_TB1 = re.compile(f"{RE_DIGIT}?\s+"
                              f"{RE_DIGIT}?\s+"
                              f"Total\s+{RE_DIGIT_TOTAL}\s+{RE_FLOAT}\s+{RE_FLOAT}\s+{RE_DIGIT}?\s+{RE_DIGIT}?\s+{RE_DIGIT}?\s+{RE_FLOAT}\s+{RE_FLOAT}\s+{RE_DIGIT_DEATHS}?\s+{RE_FLOAT}\s+"
                              f"Fonte: PAINEL COVID-19. Dados atualizados\s+")

    RE_CASES = [RE_CASES_TB1]
    resultado = None
    for re_cases in RE_CASES:
        cases_search = re_cases.search(pdf_text, re.IGNORECASE)
        if cases_search:
            groups = cases_search.groupdict()
            total  = groups.get("total")  if "total" in groups else None
            deaths = groups.get("deaths") if "deaths" in groups else None
            resultado = {
                "notified": total,
                "deaths"  : deaths
            }
            break

    return resultado


class CoronaDFSpider(scrapy.Spider):
    name = "corona-df"
    start_urls = ["http://www.saude.df.gov.br/informativos-do-centro-de-operacoes-de-emergencia-coe"]

    def parse(self, response):
        
        for link in response.xpath("//a[contains(@href, '.pdf')]"):
            boletim_titulo = link.xpath(".//text()").extract_first()

            if not "informe" in boletim_titulo.lower():
                continue

            boletim_data = getDataBoletim(link.xpath(".//text()").extract_first())
            data = {
                "boletim_titulo": boletim_titulo,
                "boletim_data": boletim_data,
                "boletim_url" : urljoin(response.url, link.xpath(".//@href").extract_first()),
            }
            
            if boletim_data <= getDataMarco01():
                yield scrapy.Request(
                    url=data["boletim_url"],
                    meta={"row": data},
                    callback=self.parse_pdf01,
                )
            elif boletim_data == getDataMarco20200327():
                yield scrapy.Request(
                    url=data["boletim_url"],
                    meta={"row": data},
                    callback=self.parse_pdf20200327,
                )                
            elif boletim_data >= getDataMarcoAtual():
                yield scrapy.Request(
                    url=data["boletim_url"],
                    meta={"row": data},
                    callback=self.parse_pdf02,
                )                


    def parse_pdf01(self, response):
        pdf_text = getPdfText(response)

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
                break

        for re_cases in RE_CASES:
            cases_search = re_cases.search(pdf_text, re.IGNORECASE)
            if cases_search:
                groups = cases_search.groupdict()
                total = groups.get("total") if "total" in groups else ""
                investigation = groups.get("investigation") if "investigation" in groups else ""
                confirmed = groups.get("confirmed")
                discarded = groups.get("discarded")
                break

        return {
            "date": f"{year}-{month}-{day}",
            "state": "DF",
            "city": "",
            "place_type": "state",
            "notified":  CleanIntegerField.deserialize(total),
            "confirmed": CleanIntegerField.deserialize(confirmed),
            "discarded": CleanIntegerField.deserialize(discarded),
            "suspect":   CleanIntegerField.deserialize(investigation),
            "deaths": 0,
            "notes": "",
            "source_url": response.url
        }

    def parse_pdf20200327(self, response):
        boletim_data = response.meta["row"]["boletim_data"]
        pdf_text = getPdfText(response)
        result = buscaTextoTabela20200327(pdf_text)
        return {
            "date": f"{boletim_data.year}-{boletim_data.month:02d}-{boletim_data.day:02d}",
            "state": "DF",
            "city": "",
            "place_type": "state",
            "notified":  result["notified"],
            "confirmed": None,
            "discarded": None,
            "suspect":   None,
            "deaths": result["deaths"],
            "notes": "",
            "source_url": response.url
        }

    def parse_pdf02(self, response):
        boletim_data = response.meta["row"]["boletim_data"]
        pdf_text = getPdfText(response)
        result = buscaTextoTabela01Geral(pdf_text)
        return {
            "date": f"{boletim_data.year}-{boletim_data.month:02d}-{boletim_data.day:02d}",
            "state": "DF",
            "city": "",
            "place_type": "state",
            "notified":  result["notified"],
            "confirmed": None,
            "discarded": None,
            "suspect":   None,
            "deaths": result["deaths"],
            "notes": "",
            "source_url": response.url
        }