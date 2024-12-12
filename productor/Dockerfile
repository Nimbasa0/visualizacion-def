
FROM pytorch/pytorch

RUN apt update -y
RUN apt upgrade -y

RUN apt-get install -y libgl1-mesa-glx
RUN apt-get install -y libglib2.0-0
RUN pip install confluent-kafka kafka-python opencv-python Pillow dask[distributed] ultralytics


WORKDIR /scripts

COPY ./scripts /scripts


CMD ["python", "productor.py"]