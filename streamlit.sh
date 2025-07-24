#!/bin/bash
pip install --upgrade pip
pip uninstall -y orjson streamlit openai || true

# 의존성 설치 (이미 빌드 단계에서 설치되었을 수 있음)
if [ ! -d "/home/site/wwwroot/.venv" ]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

# ip install --no-cache-dir orjson==3.9.10
# pip install --no-cache-dir streamlit==1.31.0
pip install -r requirements.txt
python -m streamlit run main.py --server.port 8000 --server.address 0.0.0.0