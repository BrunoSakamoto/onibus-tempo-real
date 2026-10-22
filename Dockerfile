FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

COPY pyproject.toml ./
COPY onibus ./onibus
RUN pip install .

COPY gravacoes ./gravacoes

# a mesma imagem roda os três serviços; o comando vem do docker-compose
CMD ["python", "-m", "onibus", "painel"]
