FROM python:3.9-windowsservercore

WORKDIR "C:\\app"
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN curl.exe -o curl.exe https://curl.haxx.se/windows/dl-7.83.1_5/curl-7.83.1_5-win64-mingw.zip

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]