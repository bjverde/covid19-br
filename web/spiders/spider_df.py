from collections import OrderedDict

from datetime import datetime
import locale

import io

import rows
import scrapy

from .base import BaseCovid19Spider

class Covid19DFSpider(BaseCovid19Spider):
    name = "DF"
    start_urls = ["http://www.saude.df.gov.br/boletinsinformativos-divep-cieves/"]
    
    def parse_pdf(self, response):
        mangled_table = rows.import_from_pdf(
            io.BytesIO(response.body),
            page_numbers=[1],
            starts_after='Óbitos'
        )[:2]
        
        mangled_data, mangled_total = [
            section[0].split("\n") for section in mangled_table
        ]
        
        fields = OrderedDict([
            (field_name, rows.fields.IntegerField) for \
                field_name in mangled_data[4::5]+[mangled_total[-1]]
        ])
        
        casos = rows.Table(fields=fields)
        
        casos.append(dict(list(zip( 
            casos.fields, 
            [ 
                int(num.replace(".", "")) for \
                    num in mangled_data[::5]+[mangled_total[0]] 
            ] 
        ))))
        
        obitos = rows.Table(fields=fields)
        
        obitos.append(dict(list(zip( 
            obitos.fields, 
            [ 
                int(num.replace(".", "")) for \
                    num in mangled_data[2::5]+[mangled_total[2]] 
            ] 
        ))))
        
        # Brasília
        self.add_city_case(
            city = "Brasília",
            # city_ibge_code = 5300108,
            confirmed = casos[0].distrito_federal,
            # date = date,
            deaths = obitos[0].distrito_federal,
        )
        
        # Importados/indefinidos
        self.add_city_case(
            city = "Importados/Indefinidos",
            # city_ibge_code = None,
            confirmed = sum([
                getattr(casos[0],field_name) for \
                    field_name in casos.fields if \
                        field_name not in ['distrito_federal', 'total']
            ]),
            # date = date,
            deaths = sum([
                getattr(obitos[0],field_name) for \
                    field_name in obitos.fields if \
                        field_name not in ['distrito_federal', 'total']
            ]),
        )
        
        # Total
        self.add_state_case(
            confirmed = casos[0].total,
            # date = date,
            deaths = obitos[0].total,
        )
    
    def parse(self, response):
        title = response.xpath(
            "//div[@id='conteudo']//a//text()"
        ).extract_first()
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        date = datetime.strptime(title.split()[-1],'%d%b%y')
        
        pdf_url = response.xpath(
            "//div[@id='conteudo']//a/@href"
        ).extract_first()
        self.add_report(date=date, url=pdf_url)
        
        return scrapy.Request(pdf_url, callback=self.parse_pdf)
    
