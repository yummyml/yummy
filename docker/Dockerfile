FROM python:3.9.9
RUN apt update && apt install curl git ssh cmake ack g++ tmux ncurses-dev -yq
#install vim
RUN git clone https://github.com/vim/vim.git
RUN cd ./vim && ./configure --with-features=huge \
            --enable-multibyte \
            --enable-python3interp=dynamic \
            --enable-cscope \
            --enable-fail-if-missing \
            --prefix=/usr/local && make install

RUN apt update && apt install curl git ssh cmake ack g++ tmux ncurses-dev -yq

#RUN git clone -b feature/dask https://github.com/qooba/feast.git
#RUN cd feast && pip install -e "sdk/python[ci]"
#RUN cd feast/sdk/python && pip install -e .
RUN pip install ray[default,data]==1.9.1 scikit-learn==1.0.2 dask[distributed]==2021.11.0 feast==0.18.0
RUN pip install polars
RUN pip install jupyterlab

# CREATE USER
RUN useradd -ms /bin/bash jovyan
ARG NB_USER=jovyan
ARG NB_UID=1000
ARG NB_GID=100

USER 1000
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/qooba/vim-python-ide/master/setup.sh)"

USER root
RUN pip install deltalake==0.5.6
RUN pip install twine

USER 1000

WORKDIR /home/jovyan
CMD /bin/bash
