FROM python:3.7-stretch

# ensure local python is preferred over distribution python
ENV PATH /usr/local/bin:$PATH

# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
ENV LANG C.UTF-8
WORKDIR "/app"
COPY * /app/

RUN pip3 install -r requirements.txt
CMD ["python3", "/app/crawler.py"]
