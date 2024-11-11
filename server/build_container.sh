docker run -it --gpus all -p 5000:5000 --runtime=nvidia \
    -v /mnt/jfs-hdd/share/models/LivePortrait/pretrained_weights:/workspace/LivePortrait/pretrained_weights \
    -v /home/fangyaozheng/fyz/LivePortrait/server:/workspace/LivePortrait/server \
    --name live-portrait liveportrait:latest bash

# docker exec -it a7c6a5cf7506 /bin/bash
