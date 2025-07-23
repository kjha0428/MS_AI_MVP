#!/bin/bash
pip install --upgrade pip
pip uninstall -y orjson streamlit
# ip install --no-cache-dir orjson==3.9.10
# pip install --no-cache-dir streamlit==1.31.0
pip install -r requirements.txt
python -m streamlit run main.py --server.port 8000 --server.address 0.0.0.0