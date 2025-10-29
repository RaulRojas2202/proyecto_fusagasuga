from kafka import KafkaProducer
import csv
import time
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_file = "/home/vboxuser/proyecto_fusagasuga/datasets/fuentes_hidricas_fusagasuga.csv"

with open(csv_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        # Crear un diccionario con los campos necesarios
        dato = {
            "ID": str(i),
            "Municipio": row.get("Municipio", "FUSAGASUGA"),
            "Nombre del Punto": row.get("Nombre del Punto", ""),
            "Fuente Hídrica": row.get("Fuente Hídrica ", ""),
            # Convertir a float, si no se puede usar None
            "Temperatura": float(row.get("Temperatura", 0)) if row.get("Temperatura") else None,
            "pH": float(row.get("pH", 0)) if row.get("pH") else None,
            "Fecha": row.get("Fecha", "")
        }

        # Enviar al topic
        producer.send('sensor_streaming', dato)
        print(f"📡 Enviando: {dato}")
        time.sleep(2)  # pausa de 2 segundos

producer.flush()
producer.close()
