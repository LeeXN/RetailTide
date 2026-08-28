FROM node:22-bookworm-slim AS node-runtime

WORKDIR /opt/node-deps
RUN npm init -y \
    && npm install --omit=dev --save-exact crypto-js@4.2.0 \
    && npm cache clean --force

FROM alpine/git:2.49.1 AS spider-source

ARG SPIDER_XHS_COMMIT=e1888d712519040f5fcc294baeac4b9505b25c98
RUN git clone --filter=blob:none --no-checkout https://github.com/cv-cat/Spider_XHS.git /opt/Spider_XHS \
    && git -C /opt/Spider_XHS fetch --depth=1 origin "${SPIDER_XHS_COMMIT}" \
    && git -C /opt/Spider_XHS checkout --detach FETCH_HEAD \
    && rm -rf /opt/Spider_XHS/.git

FROM python:3.11-slim-bookworm

ARG SPIDER_XHS_COMMIT=e1888d712519040f5fcc294baeac4b9505b25c98

COPY --from=node-runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=spider-source /opt/Spider_XHS /opt/Spider_XHS
COPY --from=node-runtime /opt/node-deps/node_modules /opt/Spider_XHS/node_modules

RUN pip install --no-cache-dir \
    fastapi==0.116.1 \
    uvicorn==0.35.0 \
    requests==2.32.4 \
    curl_cffi==0.15.0 \
    loguru==0.7.3 \
    python-dotenv==1.1.1 \
    retry==0.9.2

COPY app.py /opt/bridge/app.py
WORKDIR /opt/Spider_XHS
ENV PYTHONPATH=/opt/Spider_XHS \
    PYTHONUNBUFFERED=1 \
    XHS_COOKIE_FILE=/run/secrets/xhs-cookies.json

EXPOSE 18061
CMD ["uvicorn", "bridge.app:app", "--app-dir", "/opt", "--host", "0.0.0.0", "--port", "18061", "--no-access-log"]
