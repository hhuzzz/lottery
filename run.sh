#!/bin/bash

# 激活 conda 环境
source activate lottery

# 启动 gunicorn
nohup gunicorn -w 4 --bind 0.0.0.0:5000 app:app --timeout 120 --access-logfile - --error-logfile - &
