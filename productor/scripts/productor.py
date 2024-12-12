import threading
import queue
import time
import cv2
from datetime import datetime
from kafka import KafkaProducer
import json
import base64
from ultralytics import YOLO
import dask
from dask.distributed import Client
import torch 
from torchvision import transforms


def image_to_base64(image):
    _, buffer = cv2.imencode('.jpg', image)
    image_bytes = buffer.tobytes()
    image_base64 = base64.b64encode(image_bytes).decode('utf-8')
    return image_base64

def enviar_json_a_kafka(json_data, topic_name, producer):
    producer.send(topic_name, value=json_data)
    print(f"JSON enviado a Kafka, tópico: {topic_name}", flush=True)


topic_to_cam_id = {
    "processed_frames1": 1,
    "processed_frames2": 2,
    "processed_frames3": 3
}


device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f"Utilizando el dispositivo: {device}")

#modelo YOLO
model = YOLO('best.pt')  
model.to(device) 


def process_frame_for_yolo(frame_resized):
   
    frame_rgb = cv2.cvtColor(frame_resized, cv2.COLOR_BGR2RGB)
    frame_rgb = frame_rgb.astype('float32') / 255.0

    
    transform = transforms.Compose([
        transforms.ToTensor(),  
    ])
    
   
    frame_tensor = transform(frame_rgb)  
   
    frame_tensor = frame_tensor.unsqueeze(0).to(device)
    
    return frame_tensor



def procesar_stream(rtsp_url, nombre_topico):
    #KAFKA
    producer = KafkaProducer(bootstrap_servers='broker:29092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    
    cap = cv2.VideoCapture(rtsp_url)
    
    #FPS
    fps_limit = 10  
    frame_interval = 1.0 / fps_limit
    frame_count = 0 


    cam_id = topic_to_cam_id.get(nombre_topico, 0)

    
    frame_queue = queue.Queue(maxsize=1)

    
    def leer_frames():
        nonlocal frame_count
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                print(f"No se pudo leer el frame del stream {rtsp_url}. Saliendo...")
                break
           
            frame_resized = cv2.resize(frame, (640, 640))
           
            if not frame_queue.full():
                frame_queue.put(frame_resized)
            else:
               
                frame_queue.get()
                frame_queue.put(frame_resized)
            time.sleep(0)  

    
    def procesar_frame():
        nonlocal frame_count
        while cap.isOpened():
            if not frame_queue.empty():
                
                frame_resized = frame_queue.get()
                timestamp = datetime.now().isoformat()
                start_time = time.time()

             
                frame_tensor = process_frame_for_yolo(frame_resized)
                results = model(frame_tensor)  
                result = results[0]

                
                frame_resized2 = result.plot()

                
                frame_base64 = image_to_base64(frame_resized2)

                
                frame_data = {
                    "cam_id": cam_id,
                    "timestamp": timestamp,
                    "frame_base64": frame_base64,
                    "frame_id": frame_count + 1  
                }

                
                enviar_json_a_kafka(frame_data, nombre_topico, producer)
                print(f'Frame {frame_count + 1} enviado a Kafka desde {rtsp_url} con timestamp {timestamp}', flush=True)

                frame_count += 1
                elapsed_time = time.time() - start_time
                
                sleep_time = max(0, frame_interval - elapsed_time)
                time.sleep(sleep_time)  

    
    frame_reader_thread = threading.Thread(target=leer_frames)
    frame_processor_thread = threading.Thread(target=procesar_frame)
    
    frame_reader_thread.start()
    frame_processor_thread.start()

    
    frame_reader_thread.join()
    frame_processor_thread.join()

    cap.release()
    producer.close()

#Lista de streams RTSP
# http://142.0.109.159/axis-cgi/mjpg/video.cgi
# rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102
streams = [
    {"url": "rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102", "topic": "processed_frames1"},
    {"url": "rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102", "topic": "processed_frames2"},
    {"url": "rtsp://admin:test1122@129.222.172.5:554/Streaming/Channels/102", "topic": "processed_frames3"}
]


if __name__ == '__main__':
    client = Client()

    futures = []
    for stream in streams:
        future = dask.delayed(procesar_stream)(stream["url"], stream["topic"])
        futures.append(future)

    dask.compute(*futures)

    client.close()
