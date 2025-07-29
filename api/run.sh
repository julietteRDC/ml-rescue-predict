docker run -it \
-v "$(pwd):/src" \
-e MLFLOW_TRACKING_URI="https://littlerobinson-mlflow.hf.space" \
-e PORT=8888 \
-p 8888:8888 \
rescue-predict-api