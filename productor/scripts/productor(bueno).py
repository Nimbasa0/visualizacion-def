from kafka import KafkaProducer
import cv2
import json
import base64
from datetime import datetime
import time
import dask
from dask.distributed import Client

# Función para convertir una imagen a una cadena base64
def image_to_base64(image):
    _, buffer = cv2.imencode('.jpg', image)
    image_bytes = buffer.tobytes()
    image_base64 = base64.b64encode(image_bytes).decode('utf-8')
    return image_base64

# Función para enviar datos a Kafka
def enviar_json_a_kafka(json_data, topic_name, producer):
    producer.send(topic_name, value=json_data)
    print(f"JSON enviado a Kafka, tópico: {topic_name}", flush=True)

# Diccionario para mapear tópicos a cam_id
topic_to_cam_id = {
    "processed_frames1": 1,
    "processed_frames2": 2,
    "processed_frames3": 3
}

# Función principal para procesar cada stream RTSP
def procesar_stream(rtsp_url, nombre_topico):
    # Configurar Kafka Producer
    producer = KafkaProducer(bootstrap_servers='broker:29092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    # Abrir la captura de video
    cap = cv2.VideoCapture(rtsp_url)
    
    # Configurar FPS
    fps_limit = 10  # Aumentar el límite de FPS
    frame_interval = 1.0 / fps_limit
    frame_count = 0  # Este contador será único para cada stream

    # Obtener el cam_id basado en el nombre del tópico
    cam_id = topic_to_cam_id.get(nombre_topico, 0)

    while cap.isOpened():
        timestamp = datetime.now().isoformat()
        start_time = time.time()
        ret, frame = cap.read()
        if not ret:
            print(f"No se pudo leer el frame del stream {rtsp_url}. Saliendo...")
            break

        frame = cv2.resize(frame, (640, 360))

        frame_base64 = image_to_base64(frame)

        # Agregar el frame_id que es específico para cada stream
        frame_data = {
            "cam_id": cam_id,
            "timestamp": timestamp,
            "frame_base64": frame_base64,
            "frame_id": frame_count + 1  # Añadir el frame_id, empieza desde 1
        }

        # Enviar el frame con el frame_id a Kafka
        enviar_json_a_kafka(frame_data, nombre_topico, producer)
        timestamp5 = datetime.now().isoformat()
        print(f'Frame {frame_count + 1} enviado a Kafka desde {rtsp_url} con timestamp {timestamp} y segundo timestamp {timestamp5}', flush=True)

        frame_count += 1
        elapsed_time = time.time() - start_time
        
        # Asegúrate de que el tiempo de espera no cause bloqueos innecesarios
        sleep_time = max(0, frame_interval - elapsed_time)
        time.sleep(sleep_time)  # Puedes ajustar esto si es necesario

    cap.release()
    producer.close()

# Lista de streams RTSP y tópicos correspondientes
streams = [
    {"url": "rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102", "topic": "processed_frames1"},
    {"url": "rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102", "topic": "processed_frames2"},
    {"url": "rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102", "topic": "processed_frames3"}
]

# Asegurarse de que este código solo se ejecute cuando se ejecute como script, no cuando sea importado
if __name__ == '__main__':
    # Crear un cliente Dask (Esto puede ser un "LocalCluster" o "Client" si no estás utilizando un clúster distribuido)
    client = Client()

    # Usar Dask para manejar la ejecución en paralelo
    futures = []
    for stream in streams:
        future = dask.delayed(procesar_stream)(stream["url"], stream["topic"])
        futures.append(future)

    # Ejecutar todas las tareas en paralelo
    dask.compute(*futures)

    # Cerrar el cliente Dask después de que se haya completado el trabajo
    client.close()
