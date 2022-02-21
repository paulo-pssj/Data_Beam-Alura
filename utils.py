import re

columns = [ 'id',
            'data_iniSE',
            'casos',
            'ibge_code',
            'cidade',
            'uf',
            'cep',
            'latitude',
            'longitude'
        ]

def transform_text_in_list(element, delimiter='|'):
    """Recebe um texto e um delimitador e retorna uma lista de elementos separados pelo delimitador"""

    return element.split(delimiter)

def transform_list_in_dict(elements, columns=columns):
    """Recebe duas listas e retorna um dicionário"""
    return dict(zip(columns, elements))

def clean_date(element):
    """Recebe um dicionário e cria um novo campo com ANO-MES.
        Retorna o mesmo dicionário com o novo campo"""
    element["ano_mes"] = '-'.join(element["data_iniSE"].split('-')[:2])

    return element

def key_uf(element):
    """Recebe um dicionário e retorna uma tupla (UF, dicionário)"""
    key = element['uf']
    return (key, element)

def casos_dengue(element):
    """recebe uma tupla e retorn uma tupla (RS-2014-02, 8.0)"""
    uf, registros = element
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro["casos"]))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def key_uf_ano_mes_de_lista(element):
    """recebe uma lista e retorna uma tupla"""
    data, mm, uf = element
    ano_mes = '-'.join(data.split('-')[:2])
    key = f"{uf}-{ano_mes}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)

    return (key, mm)

def round_mm(element):
    """recebe uma tupla e retorna uma tupla com o valor arredondado"""
    key, mm = element
    return (key, round(mm, 1))

def filter_null(element):
    """Remove elementos que tenha campos nulos"""
    key, data = element
    if all([
        data["chuvas"],
        data["dengue"]
        ]):
        return True
    return False

def descompactar(element):
    """Recebe uma tupla e retorna uma tupla descompactada"""
    key, data = element
    chuva = data['chuvas'][0]
    dengue = data['dengue'][0]
    uf, ano, mes = key.split('-')
    return (uf, ano, mes, str(chuva), str(dengue))

def prepare_csv(element, delimiter=';'):
    """recebe uma tupla e retorna uma string delimitada"""
    return f"{delimiter}".join(element)