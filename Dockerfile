# Usamos a imagem oficial do Spark
FROM apache/spark:3.5.0-scala2.12-java11-python3-r-ubuntu

USER root

# Instalar dependências Python
RUN pip3 install kafka-python pyspark

# Criar diretorias de trabalho
WORKDIR /opt/spark/work-dir

# COPIAR o código e dados para dentro da imagem
COPY src /opt/spark/work-dir/src
COPY data /opt/spark/work-dir/data
COPY requirements.txt /opt/spark/work-dir/

# Instalar requirements se existirem
RUN pip3 install -r requirements.txt