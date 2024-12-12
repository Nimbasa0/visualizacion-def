from kafka import KafkaConsumer
import logging
import base64
from PIL import Image
from io import BytesIO
import subprocess
import numpy as np
import time
from datetime import datetime
from kafka.errors import KafkaError, NoBrokersAvailable


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#CONSUMIDOR
bootstrap_servers = 'broker:29092'
topics = ['processed_frames1', 'processed_frames2', 'processed_frames3']
group_id = 'my-group'
rtsp_url = 'rtsp://rtsp-server:8554/mystream' 
fps = 10 


def create_kafka_consumer(bootstrap_servers, topics, group_id, retries=10, delay=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000
            )
            print(f"Conexión a Kafka exitosa en el intento {attempt + 1}")
            return consumer
        except NoBrokersAvailable as e:
            print(f"Error de conexión a Kafka: {e}. Reintentando en {delay} segundos...")
            time.sleep(delay)
        except KafkaError as e:
            print(f"Error en Kafka: {e}. Reintentando en {delay} segundos...")
            time.sleep(delay)
    
    print("No se pudo conectar a Kafka después de varios intentos.")
    return None

# Configuración de ffmpeg
ffmpeg_cmd = [
    'ffmpeg',
    '-y',
    '-f', 'rawvideo',
    '-pix_fmt', 'bgr24',
    '-s', '1280x720',
    '-r', str(fps),
    '-i', '-',  # Leer desde stdin
    '-c:v', 'libx264',
    '-preset', 'ultrafast',
    '-crf', '28',
    '-b:v', '500k',
    '-g', '10',
    '-f', 'rtsp',
    '-rtsp_transport', 'udp',
    '-max_interleave_delta', '100M',
    '-x264-params', 'slice-max-size=1460',
    '-loglevel', 'debug',
    '-tune', 'zerolatency',
    rtsp_url
]


ffmpeg_process = subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE)


packet_count = 1


last_frame = None


def send_last_frame(last_frame, fps):
    if last_frame is not None:
        for _ in range(int(fps)): 
            ffmpeg_process.stdin.write(last_frame)
            ffmpeg_process.stdin.flush()
            time.sleep(1 / fps)  


frames = {
    'processed_frames1': None,
    'processed_frames2': None,
    'processed_frames3': None
}


def create_composite_image(frames):
    composite = np.zeros((720, 1280, 3), dtype=np.uint8)  

    
    if frames['processed_frames1'] is not None:
        composite[0:360, 0:640] = frames['processed_frames1']
    if frames['processed_frames2'] is not None:
        composite[0:360, 640:1280] = frames['processed_frames2']
    if frames['processed_frames3'] is not None:
        composite[360:720, 0:640] = frames['processed_frames3']

    return composite.tobytes()


def resize_image(image, size=(640, 360)):
    return image.resize(size) 


consumer = create_kafka_consumer(bootstrap_servers, topics, group_id)

if not consumer:
    logger.error("No se pudo conectar a Kafka, terminando el proceso.")
    exit(1)  


try:
    logger.info('Iniciando consumo de mensajes...')
    while True:
        
        message = consumer.poll(timeout_ms=1000) 

        if message:
            for topic_partition, messages in message.items():
                topic = topic_partition.topic
                for msg in messages:
                    
                    timestamp3 = datetime.now().isoformat()

                    
                    message_value = msg.value.decode('utf-8')
                    
                    
                    try:
                        message_json = eval(message_value) 
                        frame_id = message_json.get('frame_id')  
                        timestamp2 = message_json.get('timestamp2') 
                        timestamp1 = message_json.get('timestamp1')
                        timestamp = message_json.get('timestamp')  
                        base64_data = message_json.get('frame_base64') 
                        
                        if base64_data:
                            
                            image_data = base64.b64decode(base64_data)
                            
                            
                            image = Image.open(BytesIO(image_data))

                            
                            image = resize_image(image, size=(640, 360))

                           
                            image_np = np.array(image)
                            image_np = image_np[:, :, ::-1]
                            

                            
                            frames[topic] = image_np  
                            
                            
                            logger.info(f"Frame recibido de {topic} con ID: {frame_id}")
                            logger.info(f"Timestamp: {timestamp}, Timestamp2: {timestamp2}, Timestamp3: {timestamp3}")

                            
                            composite_frame = create_composite_image(frames)

                            
                            ffmpeg_process.stdin.write(composite_frame)
                            ffmpeg_process.stdin.flush()

                            
                            logger.info(f'Paquete {packet_count} recibido de {topic}')
                            logger.info(f'Frame ID: {frame_id}, Timestamp: {timestamp}, Timestamp1: {timestamp1}, Timestamp2: {timestamp2}, Timestamp3: {timestamp3}')
                        else:
                            logger.warning(f'Paquete {packet_count} recibido de {topic} pero no contiene datos Base64.')
                        
                    except Exception as e:
                        logger.error(f'Error procesando el paquete {packet_count}: {e}')
                    
                    packet_count += 1
        else:
            
            send_last_frame(create_composite_image(frames), fps)

except KeyboardInterrupt:
    logger.info("Consumo interrumpido por el usuario.")
finally:
    
    consumer.close()
    ffmpeg_process.stdin.close()
    ffmpeg_process.wait()
    logger.info('Consumidor cerrado y proceso ffmpeg terminado.')
