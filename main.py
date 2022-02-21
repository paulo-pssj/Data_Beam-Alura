from utils import *
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "Transforma texto em lista" >> beam.Map(transform_text_in_list)
    | "Transforma lista em dicionário" >> beam.Map(transform_list_in_dict)
    | "Criar campo ANO-MES" >> beam.Map(clean_date)
    | "Criar chave pelo estado" >> beam.Map(key_uf)
    | "Agrupar pelo Estado" >> beam.GroupByKey()
    | "Descompactar casos dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos Casos de dengue por UF e ano_mes" >> beam.CombinePerKey(sum)
    # | "mostrar resultados" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | "Transforma texto em lista (chuvas)" >> beam.Map(transform_text_in_list, delimiter=',')
    | "Criando chave uf-ano_mes" >> beam.Map(key_uf_ano_mes_de_lista)
    | "Soma do total de chuva pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> beam.Map(round_mm)
    #| "mostrar resultados" >> beam.Map(print)
)

resultado = (
    ({'chuvas': chuvas, 'dengue': dengue})
    # | "empilha as pcollections" >> beam.Flatten()
    # | "Agrupa as pcols" >> beam.GroupByKey()
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "Filtra casos vazios" >> beam.Filter(filter_null)
    | "Descompactar" >> beam.Map(descompactar)
    | "Preparar CSV" >> beam.Map(prepare_csv)
    # | "Mostrar resultados" >> beam.Map(print)
)

header = "UF;ANO;MES;CHUVA;DENGUE"

resultado | "Criar arquivo CSV" >> WriteToText('resultado', file_name_suffix=".csv", header=header)

pipeline.run()