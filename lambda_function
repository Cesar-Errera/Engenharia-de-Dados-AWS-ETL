import json
import requests
# Essa é a biblioteca que te permite fazer as interações com a AWS
import boto3
# Essa biblioteca a gente precisa pra ler a chave da API
import os

# Configurações da API
latitude = -23.728991340850914
longitude = -46.568571392227504
#Criação de uma variavel local para adicionar dentro da AWS
TOMORROW_API_KEY = os.getenv('TOMORROW_API_KEY')
url = f'https://api.tomorrow.io/v4/weather/forecast?location={latitude},{longitude}&apikey={TOMORROW_API_KEY}'
headers = {"accept": "application/json"}

# Configurações do KINESIS
STREAM_NAME = "Broker"
REGION = "us-east-1" ## região padrão na aws

# Cliente do KINESIS - faz a chamada do serviço
kinesis_client = boto3.client('kinesis',region_name=REGION)

# Obrigatoriamente a função tem que se chamar "lambda_handler" pois ele procura por esse nome
def lambda_handler(event, context):
    ## Faz a requisição da API
    response = requests.get(url, headers = headers)
    # Resposta formatada para json e coloca na variavel 'wheater_data'
    weather_data = response.json()

    ## Envia os dados para o KINESIS
    kinesis_client.put_record(
        StreamName = STREAM_NAME,
        Data = json.dumps(weather_data),
        #Forma como ele vai particionar os dados dentro do broker
        PartitionKey = "partition_key"
    )

    return{
        'statusCode': 200,
        'body': json.dumps('Dados enviados ao KINESIS com sucesso!')
    }
