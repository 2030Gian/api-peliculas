import boto3
import uuid
import os
import json
import traceback

def lambda_handler(event, context):
    try:
        # --- Parseo de entrada ---
        # Asumimos que event['body'] ya es un dict; si llega como string, hacer json.loads
        body = event.get('body')
        if isinstance(body, str):
            body = json.loads(body)

        tenant_id      = body['tenant_id']
        pelicula_datos = body['pelicula_datos']
        nombre_tabla   = os.environ["TABLE_NAME"]

        # --- Inserción en DynamoDB ---
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id':       tenant_id,
            'uuid':            uuidv4,
            'pelicula_datos':  pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table    = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # --- Log de INFO ---
        log = {
            "tipo":      "INFO",
            "log_datos": {
                "mensaje":    "Pelicula creada correctamente",
                "tenant_id":  tenant_id,
                "uuid":       uuidv4,
                "tabla":      nombre_tabla
            }
        }
        print(json.dumps(log))

        # --- Respuesta HTTP ---
        return {
            'statusCode': 200,
            'body': {
                'pelicula': pelicula,
                'dynamo_response': response  # opcional: puede filtrar campos si quiere
            }
        }

    except Exception as e:
        # --- Log de ERROR ---
        error_log = {
            "tipo":      "ERROR",
            "log_datos": {
                "mensaje":   str(e),
                "traceback": traceback.format_exc()
            }
        }
        print(json.dumps(error_log))

        # --- Respuesta HTTP de error ---
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }
