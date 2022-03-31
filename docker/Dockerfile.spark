FROM jupyter/pyspark-notebook:spark-3.2.0
RUN pip3 install deltalake
env PYSPARK_DRIVER_PYTHON=jupyter
env PYSPARK_DRIVER_PYTHON_OPTS="lab --notebook-dir=/home/jovyan --ip='0.0.0.0' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token=''"


RUN pip3 install scikit-learn==1.0.2 feast==0.18.0

USER root
RUN apt update && apt install curl git ssh cmake ack g++ tmux vim -yq

USER 1000
RUN /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/qooba/vim-python-ide/master/setup.sh)"

RUN git clone https://github.com/qooba/feast-pyspark.git && cd feast-pyspark && pip install -e .

CMD pyspark --packages io.delta:delta-core_2.12:1.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.driver.memory=5g" --conf "spark.executor.memory=5g"

